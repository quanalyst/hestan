#!/usr/bin/env bash
# part 1: the image builds, and a container from it comes up and serves.
#
# no compose and no database server here. one container on sqlite, which is
# what a hestan application on one host is, and the smallest thing that can be
# wrong with an image.

source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

name="hestan-demo"
box="${project}-image"

cleanup() { docker rm -f "$box" >/dev/null 2>&1 || true; }
trap cleanup EXIT

say "== building $name"
if docker build -t "$name" "$repo" >/tmp/hestan-image-build.log 2>&1; then
    pass "the image builds"
else
    bad "the image did not build; see /tmp/hestan-image-build.log"
    tail -20 /tmp/hestan-image-build.log >&2
    done_checking
fi

say ""
say "== what got built"
disk="$(docker images "$name:latest" --format '{{.Size}}' | head -1)"
wire="$(docker image inspect "$name:latest" --format '{{.Size}}')"
note "$disk unpacked on disk, $((wire / 1000 / 1000))MB of content to pull"
note "the runtime layer, oldest first, everything that is not 0 bytes:"
docker history "$name:latest" --format '{{.Size}}|{{.CreatedBy}}' --no-trunc \
    | grep -v '^0B|' | tac \
    | awk -F'|' '{ printf "       %-8s %s\n", $1, substr($2, 1, 64) }'

say ""
say "== a container from it"
docker run -d --name "$box" -p 4100:4000 \
    -e HESTAN_ADDR=0.0.0.0:4000 -e HESTAN_TOKEN="$token" "$name" >/dev/null

up=0
for _ in $(seq 1 60); do
    if curl -fsS -H "Authorization: Bearer $token" \
        http://127.0.0.1:4100/api/health >/dev/null 2>&1; then
        up=1
        break
    fi
    sleep 1
done
if [ "$up" = "1" ]; then
    pass "the container came up"
else
    bad "the container never answered on 4100"
    docker logs "$box" 2>&1 | tail -20 >&2
    done_checking
fi

health="$(curl -fsS -H "Authorization: Bearer $token" http://127.0.0.1:4100/api/health)"
if [ "$(printf '%s' "$health" | json ok)" = "true" ]; then
    pass "/api/health answers, and its store is writing"
else
    bad "/api/health says $health"
fi

# it took the deciding lease on its own, which is what one process on one
# database has always done and what the election must not have changed
if [ "$(printf '%s' "$health" | json deciding.leader)" = "true" ]; then
    pass "one process on one database decides without waiting for anybody"
else
    bad "a single process did not take the deciding lease: $health"
fi

code_type="$(curl -fsS -o /dev/null -w '%{http_code} %{content_type}' http://127.0.0.1:4100/)"
if [ "${code_type% *}" = "200" ] && [[ "$code_type" == *"text/html"* ]]; then
    pass "the embedded ui is served at / ($code_type)"
else
    bad "GET / answered $code_type"
fi

# and the bundle the page asks for, since an index.html with no javascript
# behind it is an image that looks like it works
asset="$(curl -fsS http://127.0.0.1:4100/ | grep -o '/assets/[^"]*\.js' | head -1)"
if [ -n "$asset" ] && curl -fsS -o /dev/null "http://127.0.0.1:4100$asset"; then
    pass "the ui bundle is served too ($asset)"
else
    bad "the ui page asked for $asset and did not get it"
fi

who="$(docker exec "$box" id -u)"
if [ "$who" = "10001" ]; then
    pass "it runs as uid 10001, not root"
else
    bad "it runs as uid $who"
fi

say ""
done_checking
