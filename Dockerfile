FROM node:20-slim
RUN apt-get update && apt-get install -y --no-install-recommends ffmpeg ca-certificates && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY package.json ./
RUN npm install --omit=dev
COPY index.js ./
ENV GOOGLE_APPLICATION_CREDENTIALS=/app/serviceAccount.json
CMD ["node", "index.js"]
