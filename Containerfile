# 1. Usar uma imagem Python oficial e leve
FROM python:3.10-slim

# 2. Definir o diretório de trabalho dentro do container
WORKDIR /app

# 3. Copiar o arquivo de dependências e instalá-las
# É bom fazer isso primeiro para aproveitar o cache do Docker
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 4. Copiar todo o código fonte para o container
COPY . .

# 5. (Opcional, mas bom) Definir um usuário não-root para segurança
# RUN adduser --disabled-password --gecos '' appuser
# USER appuser

# CMD não é mais necessário aqui, pois o comando será definido no docker-compose.