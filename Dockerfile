FROM python:3.13-slim

# 1. Traz o executável do uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# 2. CONFIGURAÇÕES CRÍTICAS
ENV UV_COMPILE_BYTECODE=0
ENV UV_PROJECT_ENVIRONMENT="/usr/local"
ENV UV_PYTHON_DOWNLOADS=0
ENV UV_LINK_MODE=copy
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# 3. Instala dependências do SO
RUN apt-get update && apt-get install -y \
    curl \
    git \
    && rm -rf /var/lib/apt/lists/*

# 4. Copia arquivos de definição de dependências
COPY pyproject.toml uv.lock ./

# 5. Instala as dependências do Python
RUN uv sync --frozen --no-install-project

# 6. Copia o código do projeto
COPY . .

# 7. Instala o próprio projeto
RUN uv sync --frozen

# Prefect default port
EXPOSE 4200
