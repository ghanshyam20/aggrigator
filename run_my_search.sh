#!/usr/bin/env bash
# run_my_search.sh — filters: Warehouse/Kitchen/Cleaning/Fast-food in Helsinki/Espoo/Vantaa
set -e
cd "$(dirname "$0")"
source .venv/bin/activate

python fin_jobs_aggregator.py --sites sites.yaml \
  --keywords "warehouse,warehousing,logistics,logistic,material handler,picker,packer,forklift,siivous,siivooja,toimitilahuoltaja,cleaner,cleaning,janitor,housekeeping,keittio,keittiö,keittiöapulainen,astiahuoltaja,tiskaaja,kitchen,dishwasher,cook,chef,ravintola,fast food,pikaruoka,crew,cashier,McDonalds,Hesburger,Burger King,Subway,Taco Bell" \
  --locations "Helsinki,Espoo,Vantaa" \
  --max-per-site 80 \
  --out-csv jobs.csv --out-json jobs.json \
  --out-html jobs.html --out-links links.txt

# Open results (try gio first, then xdg)
(gio open jobs.html || xdg-open jobs.html) >/dev/null 2>&1 || true
