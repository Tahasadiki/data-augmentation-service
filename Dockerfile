FROM python:3.9-slim as base

WORKDIR /app

COPY . /app

RUN pip install --no-cache-dir -r requirements.txt

RUN useradd -m appuser
USER appuser

# Stage for S3 Watcher
FROM base as watcher
CMD ["python", "src/main.py", "watcher"]

# Stage for Batch Processor
FROM base as processor
CMD ["python", "src/main.py", "processor"]

# Final stage
FROM base as final

# Use ARG to specify which service to build
ARG SERVICE=processor

COPY --from=watcher /app /app
COPY --from=processor /app /app


# Run the specified service when the container launches
CMD python src/main.py ${SERVICE}
