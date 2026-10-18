# Official multi-platform manifest digest verified from registry-1.docker.io.
FROM python:3.11-slim-bookworm@sha256:0a310eeecf4e1f5a0743f9a6520c90c88d089c903ca5fd283f501e3a805f5f89 AS builder
WORKDIR /build
COPY constraints /build/constraints
RUN python -m pip install --no-cache-dir -c constraints/build.txt build setuptools wheel
COPY pyproject.toml README.md /build/
COPY k8s_inspector /build/k8s_inspector
RUN python -m build --wheel --no-isolation --outdir /wheel

FROM python:3.11-slim-bookworm@sha256:0a310eeecf4e1f5a0743f9a6520c90c88d089c903ca5fd283f501e3a805f5f89
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 HOME=/tmp
WORKDIR /app
COPY constraints/runtime.txt /constraints/runtime.txt
COPY --from=builder /wheel /wheel
RUN python -m pip install --no-cache-dir -c /constraints/runtime.txt /wheel/*.whl && python -m pip check
USER 10001:10001
ENTRYPOINT ["python", "-m", "k8s_inspector.cli"]
