FROM debian:bookworm-slim

ENV VIRTUAL_ENV=/usr/local
ENV PATH="/root/.local/bin/:$PATH"

WORKDIR /app

SHELL ["/bin/bash", "-c"]

RUN apt-get update && apt-get install -y curl wget && \
    curl https://sh.rustup.rs -sSf | sh -s -- -y && \
    apt-get autoremove && apt-get clean && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

ADD https://astral.sh/uv/install.sh /uv-installer.sh

RUN sh /uv-installer.sh && rm /uv-installer.sh && \
    uv venv env --python=python3.12

COPY ["api.py", \
        "db.py", \
        "service.py", \
        "pyproject.toml", \
        "supervisord.conf", \
        "/app/"]

COPY templates /app/templates

RUN source env/bin/activate && \
    uv sync && \
    rm -rf $HOME/.cache/uv && \
    mkdir log && mkdir log/sv_child && mkdir run

# run container
CMD ["/bin/bash", "-c", "source env/bin/activate && uv run supervisord -c supervisord.conf"]

