#!/usr/bin/env bash
# 质量裁剪端到端演示：尾部低质量裁剪、整对舍弃、失败保留旧包、调整阈值重算。
# 依赖: curl、python3；样例先由 scripts/make_samples.py 生成（lowqual/）。
set -euo pipefail

BASE="${BASE_URL:-http://127.0.0.1:8000}"
SAMPLES="${SAMPLES_DIR:-samples}"

json_get() { python3 -c "import json,sys; d=json.load(sys.stdin); print($1)"; }

wait_job() {  # $1 = batch_id → 轮询直到任务离开 running，打印最终状态
  local status
  for _ in $(seq 1 100); do
    status="$(curl -sS "$BASE/batches/$1/trim" | json_get 'd["job"]["status"] if d["job"] else "none"')"
    [[ "$status" == "running" ]] || { echo "$status"; return 0; }
    sleep 0.1
  done
  echo "timeout"; return 1
}

post_trim() {  # $1 = batch_id $2 = min_quality $3 = min_length
  curl -sS -o /tmp/fastq_trim_resp.json -w "  HTTP %{http_code}\n" \
    -X POST "$BASE/batches/$1/trim" \
    -H 'Content-Type: application/json' \
    -d "{\"min_quality\":$2,\"min_length\":$3}"
}

show_trim_status() {  # $1 = batch_id
  curl -sS "$BASE/batches/$1/trim" | python3 -c '
import json, sys
d = json.load(sys.stdin)
j, p = d["job"], d["package"]
if j:
    line = "  job: {}  min_quality={} min_length={}  pairs_in={} pairs_kept={}".format(
        j["status"], j["min_quality"], j["min_length"], j["pairs_in"], j["pairs_kept"])
    if j["error"]:
        line += "  error=" + j["error"]
    print(line)
if p:
    print("  package: job={}…  pairs_kept={}  size={} bytes".format(
        p["job_id"][:8], p["pairs_kept"], p["size"]))
'
}

echo "服务: $BASE"
curl -sS "$BASE/health" | python3 -m json.tool

echo "================ 上传低质量尾样例 ================"
BID="$(curl -sS -X POST "$BASE/batches" -H 'Content-Type: application/json' \
  -d '{"sample_id":"demo-trim"}' | json_get 'd["id"]')"
echo "batch: $BID"
for role in R1 R2; do
  echo "  upload $role"
  curl -sS -o /dev/null -w "  HTTP %{http_code}\n" -X POST "$BASE/batches/$BID/files/$role" \
    -H 'Content-Type: application/gzip' --data-binary "@$SAMPLES/lowqual/$role.fastq.gz"
done
curl -sS "$BASE/batches/$BID" | json_get 'd["status"]'

echo
echo "裁剪前第 1 条 R1（注意质量串尾部的 '!' 低质量区，Phred+33 Q0）:"
curl -sS "$BASE/batches/$BID/files/R1/download" | head -n 4

echo
echo "================ 提交裁剪任务 min_quality=30 min_length=50 ================"
post_trim "$BID" 30 50
echo "任务状态: $(wait_job "$BID")"
show_trim_status "$BID"

echo
echo "裁剪后第 1 条 R1（尾部低质量碱基已裁去；每第 5 对因不足 50bp 被整对舍弃）:"
curl -sS "$BASE/batches/$BID/trim/files/R1/download" | head -n 4
echo "结果包 R2 同样可下载:"
curl -sS -o /dev/null -w "  HTTP %{http_code}\n" "$BASE/batches/$BID/trim/files/R2/download"

echo
echo "================ 失败保留：min_length=81（无读对可存活） ================"
post_trim "$BID" 30 81
echo "任务状态: $(wait_job "$BID")"
show_trim_status "$BID"
echo "旧结果包在失败期间与之后仍可下载（内容不变）:"
curl -sS "$BASE/batches/$BID/trim/files/R1/download" | head -n 4

echo
echo "================ 调整阈值重算：min_quality=30 min_length=30 ================"
post_trim "$BID" 30 30
echo "任务状态: $(wait_job "$BID")"
show_trim_status "$BID"
echo "新结果包已原子替换旧包；原始文件与批次状态始终不变:"
curl -sS "$BASE/batches/$BID" | json_get 'd["status"]'
