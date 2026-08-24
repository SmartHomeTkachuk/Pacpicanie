FROM python:3.11-slim

WORKDIR /app

# Устанавливаем зависимости
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копируем код
COPY main.py .

# Открываем порт (для Flask)
EXPOSE 8080

# Запускаем бота
CMD ["python", "main.py"]
