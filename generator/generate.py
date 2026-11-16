"""Genera un dataset retail sintético multi-sistema con problemas típicos de ingesta Bronze.

Fuentes:
  crm/        snapshot diario CSV de clientes (PII, duplicados con variantes)
  erp_cdc/    eventos CDC estilo Debezium (c/u/d) de customers y orders:
              duplicados, entregas fuera de orden, deletes
  app_events/ JSONL por hora (muchos archivos pequeños): llegadas tardías,
              schema drift a mitad del periodo, tipos inconsistentes, líneas corruptas
  truth/      respuestas correctas para validar dedup y survivorship
"""
import argparse
import csv
import json
import random
import unicodedata
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

from faker import Faker

ORDER_FLOW = ["created", "paid", "shipped", "delivered"]


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--customers", type=int, default=20_000)
    p.add_argument("--orders-per-day", type=int, default=3_000)
    p.add_argument("--days", type=int, default=30)
    p.add_argument("--start", default="2026-01-01")
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--out", default="data")
    return p.parse_args()


def strip_accents(s):
    return "".join(c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn")


def name_variant(name, rnd):
    return rnd.choice([
        name.upper(),
        strip_accents(name),
        name.replace(" ", "  ", 1),
        strip_accents(name).lower().title(),
    ])


def phone_variant(phone, rnd):
    d = "".join(c for c in phone if c.isdigit())[-10:]
    return rnd.choice([d, f"+52 {d[:2]} {d[2:6]} {d[6:]}", f"({d[:2]}) {d[2:6]}-{d[6:]}"])


def ts_ms(dt):
    return int(dt.timestamp() * 1000)


class Generator:
    def __init__(self, args):
        self.a = args
        self.rnd = random.Random(args.seed)
        self.fake = Faker("es_MX")
        self.fake.seed_instance(args.seed)
        self.start = datetime.fromisoformat(args.start).replace(tzinfo=timezone.utc)
        self.out = Path(args.out)
        self.golden = {}        # golden_id -> estado real del cliente
        self.crm = {}           # crm_id -> {golden_id, name, email, phone, ...}
        self.crm_by_gid = defaultdict(list)
        self.erp_ids = {}       # golden_id -> erp customer_id
        self.erp_state = {}     # erp_id -> fila actual en ERP
        self.orders = {}        # order_id -> fila actual en ERP
        self.order_seq = 0
        self.whale = None
        self.cdc_buffer = defaultdict(list)   # día de entrega -> eventos
        self.app_buffer = defaultdict(list)   # día de entrega -> eventos

    # ---------- clientes ----------
    def new_customer(self, gid, when):
        f, r = self.fake, self.rnd
        c = {
            "golden_id": gid,
            "name": f"{f.first_name()} {f.last_name()} {f.last_name()}",
            "email": f"{f.user_name()}{r.randint(1, 999)}@{f.free_email_domain()}",
            "phone": f.numerify("55########"),
            "rfc": f.rfc(),
            "birth_date": f.date_of_birth(minimum_age=18, maximum_age=80).isoformat(),
            "city": f.city(),
            "address": f.street_address(),
            "segment": r.choice(["retail", "retail", "retail", "pyme", "vip"]),
        }
        self.golden[gid] = c

        # CRM: alta + 6% de duplicados con variantes
        self.add_crm(c, when, variant=False)
        if r.random() < 0.06:
            self.add_crm(c, when + timedelta(days=r.randint(0, 5)), variant=True)

        # ERP: 85% de los clientes llegan a comprar
        if r.random() < 0.85:
            eid = f"E{len(self.erp_ids) + 100000}"
            self.erp_ids[gid] = eid
            row = {
                "customer_id": eid,
                "full_name": c["name"].upper(),           # el ERP guarda en mayúsculas
                "email": c["email"].lower(),
                "phone": None if r.random() < 0.3 else c["phone"],
                "tax_id": c["rfc"],
                "billing_address": c["address"],
                "city": c["city"],
                "status": "active",
                "updated_at": when.isoformat(),
            }
            self.erp_state[eid] = row
            self.cdc("customers", "c", None, row, when)

    def add_crm(self, c, when, variant):
        r = self.rnd
        crm_id = f"CRM-{len(self.crm) + 1:07d}"
        self.crm_by_gid[c["golden_id"]].append(crm_id)
        self.crm[crm_id] = {
            "crm_id": crm_id,
            "golden_id": c["golden_id"],
            "nombre": name_variant(c["name"], r) if variant else c["name"],
            "email": (c["email"].upper() + " ") if variant and r.random() < 0.5 else c["email"],
            "telefono": phone_variant(c["phone"], r) if variant else c["phone"],
            "rfc": c["rfc"] if r.random() > 0.1 else "",          # RFC faltante
            "fecha_nacimiento": c["birth_date"],
            "ciudad": c["city"],
            "segmento": c["segment"],
            "fecha_alta": when.date().isoformat(),
            "ultima_modificacion": when.isoformat(),
        }

    def mutate_customers(self, day_dt):
        r, f = self.rnd, self.fake
        gids = list(self.golden)
        for gid in r.sample(gids, k=max(1, len(gids) // 100)):
            c = self.golden[gid]
            when = day_dt + timedelta(seconds=r.randint(0, 86_399))
            change = r.choice(["phone", "address", "email"])
            if change == "phone":                  # el cambio de teléfono llega solo al CRM
                c["phone"] = f.numerify("55########")
                for cid in self.crm_by_gid[gid]:
                    self.crm[cid]["telefono"] = c["phone"]
                    self.crm[cid]["ultima_modificacion"] = when.isoformat()
            elif change == "address":              # la dirección cambia solo en el ERP
                c["address"], c["city"] = f.street_address(), f.city()
                self.erp_update(gid, when, billing_address=c["address"], city=c["city"])
            else:                                  # email cambia en ambos, el CRM se entera días después
                c["email"] = f"{f.user_name()}{r.randint(1, 999)}@{f.free_email_domain()}"
                self.erp_update(gid, when, email=c["email"].lower())
                for cid in self.crm_by_gid[gid]:
                    self.crm[cid]["email"] = c["email"]

        # 0.1% de bajas en ERP (delete físico)
        for gid in r.sample(list(self.erp_ids), k=max(0, len(self.erp_ids) // 1000)):
            eid = self.erp_ids.pop(gid)
            before = self.erp_state.pop(eid)
            self.cdc("customers", "d", before, None, day_dt + timedelta(hours=r.randint(0, 23)))
            self.golden[gid]["erp_deleted"] = True

    def erp_update(self, gid, when, **changes):
        eid = self.erp_ids.get(gid)
        if not eid:
            return
        before = dict(self.erp_state[eid])
        after = {**before, **changes, "updated_at": when.isoformat()}
        self.erp_state[eid] = after
        self.cdc("customers", "u", before, after, when)

    # ---------- pedidos ----------
    def pick_buyer(self):
        if self.rnd.random() < 0.10:               # skew: un cliente "ballena" con 10% de pedidos
            return self.whale
        return self.rnd.choice(list(self.erp_ids))

    def place_orders(self, day_idx, day_dt):
        r = self.rnd
        drift = day_idx >= self.a.days // 2
        for _ in range(self.a.orders_per_day):
            gid = self.pick_buyer()
            if gid not in self.erp_ids:
                continue
            self.order_seq += 1
            oid = f"ORD-{self.order_seq:09d}"
            when = day_dt + timedelta(seconds=r.randint(0, 86_399))
            amount = round(r.lognormvariate(6, 0.9), 2)
            row = {
                "order_id": oid,
                "customer_id": self.erp_ids[gid],
                "amount": amount,
                "currency": "MXN",
                "status": "created",
                "updated_at": when.isoformat(),
            }
            self.orders[oid] = row
            self.cdc("orders", "c", None, row, when)

            # transiciones de estado en días siguientes
            t = when
            final = 4 if r.random() > 0.08 else 1
            for status in ORDER_FLOW[1:final]:
                t += timedelta(hours=r.randint(1, 48))
                self.order_status(oid, status, t)
            if final == 1:
                self.order_status(oid, "cancelled", t + timedelta(hours=r.randint(1, 24)))

            # evento de la app
            ev = {
                "event_id": f"{oid}-placed",
                "event_type": "order_placed",
                "event_time": when.isoformat(),
                "order_id": oid,
                "customer_email": self.golden[gid]["email"],
                "amount": str(amount) if r.random() < 0.02 else amount,   # tipo inconsistente
                "items": r.randint(1, 8),
            }
            if drift:                                  # schema drift a mitad del periodo
                ev["device"] = r.choice(["ios", "android", "web"])
                ev["coupon_code"] = r.choice([None, None, None, "BUENFIN", "ENVIOGRATIS"])
            self.app(ev, when)

    def order_status(self, oid, status, when):
        before = dict(self.orders[oid])
        after = {**before, "status": status, "updated_at": when.isoformat()}
        self.orders[oid] = after
        self.cdc("orders", "u", before, after, when)

    # ---------- emisión con entregas desordenadas ----------
    def delivery_day(self, when, late_prob):
        day = max(0, (when.date() - self.start.date()).days)   # histórico -> carga inicial
        return day + 1 if self.rnd.random() < late_prob else day

    def cdc(self, table, op, before, after, when):
        ev = {
            "op": op,
            "ts_ms": ts_ms(when),
            "source": {"system": "erp", "db": "erp_prod", "table": table},
            "before": before,
            "after": after,
        }
        day = self.delivery_day(when, late_prob=0.03)
        self.cdc_buffer[day].append(ev)
        if self.rnd.random() < 0.01:                   # reentrega duplicada
            self.cdc_buffer[day].append(ev)

    def app(self, ev, when):
        self.app_buffer[self.delivery_day(when, late_prob=0.02)].append((when.hour, ev))

    # ---------- escritura ----------
    def write_day(self, day_idx, day_dt):
        d = day_dt.date().isoformat()

        crm_dir = self.out / "crm"
        crm_dir.mkdir(parents=True, exist_ok=True)
        rows = [{k: v for k, v in row.items() if k != "golden_id"}
                for row in self.crm.values() if row["fecha_alta"] <= d]
        with open(crm_dir / f"clientes_{d}.csv", "w", newline="", encoding="utf-8") as fh:
            w = csv.DictWriter(fh, fieldnames=list(rows[0]))
            w.writeheader()
            w.writerows(rows)

        events = self.cdc_buffer.pop(day_idx, [])
        self.rnd.shuffle(events)                       # el orden de archivo no es el orden real
        cdc_dir = self.out / "erp_cdc" / f"date={d}"
        cdc_dir.mkdir(parents=True, exist_ok=True)
        for part in range(0, len(events), 5_000):
            with open(cdc_dir / f"cdc-{part // 5_000:04d}.json", "w", encoding="utf-8") as fh:
                for ev in events[part:part + 5_000]:
                    fh.write(json.dumps(ev, ensure_ascii=False) + "\n")

        by_hour = defaultdict(list)
        for hour, ev in self.app_buffer.pop(day_idx, []):
            by_hour[hour].append(ev)
        for hour, evs in by_hour.items():
            path = self.out / "app_events" / f"date={d}" / f"hour={hour:02d}"
            path.mkdir(parents=True, exist_ok=True)
            with open(path / "events.jsonl", "w", encoding="utf-8") as fh:
                for ev in evs:
                    line = json.dumps(ev, ensure_ascii=False)
                    if self.rnd.random() < 0.003:      # línea corrupta
                        line = line[: self.rnd.randint(10, len(line) - 5)]
                    fh.write(line + "\n")

    def write_truth(self):
        t = self.out / "truth"
        t.mkdir(parents=True, exist_ok=True)
        with open(t / "customers_golden.csv", "w", newline="", encoding="utf-8") as fh:
            w = csv.writer(fh)
            w.writerow(["golden_id", "name", "email", "phone", "rfc", "city", "address",
                        "segment", "erp_customer_id", "erp_deleted"])
            for gid, c in self.golden.items():
                w.writerow([gid, c["name"], c["email"], c["phone"], c["rfc"], c["city"],
                            c["address"], c["segment"], self.erp_ids.get(gid, ""),
                            c.get("erp_deleted", False)])
        with open(t / "crm_xref.csv", "w", newline="", encoding="utf-8") as fh:
            w = csv.writer(fh)
            w.writerow(["crm_id", "golden_id"])
            w.writerows((k, v["golden_id"]) for k, v in self.crm.items())

    def run(self):
        a, r = self.a, self.rnd
        initial = int(a.customers * 0.7)
        signups = sorted(r.randint(1, a.days - 1) for _ in range(a.customers - initial))
        gid = 0
        for _ in range(initial):
            gid += 1
            self.new_customer(f"G{gid:07d}", self.start - timedelta(days=r.randint(1, 365)))
        self.whale = r.choice(list(self.erp_ids))

        for day_idx in range(a.days):
            day_dt = self.start + timedelta(days=day_idx)
            while signups and signups[0] == day_idx:
                signups.pop(0)
                gid += 1
                self.new_customer(f"G{gid:07d}", day_dt + timedelta(seconds=r.randint(0, 86_399)))
            if day_idx > 0:
                self.mutate_customers(day_dt)
            self.place_orders(day_idx, day_dt)
            self.write_day(day_idx, day_dt)
            print(f"{day_dt.date()} listo")

        self.write_truth()


def main():
    Generator(parse_args()).run()


if __name__ == "__main__":
    main()
