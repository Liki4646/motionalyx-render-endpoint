FROM node:20-bookworm-slim

# Install ffmpeg + basic fonts + CA certificates
RUN apt-get update && apt-get install -y --no-install-recommends \
  ffmpeg \
  fonts-dejavu-core \
  ca-certificates \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY package.json ./
RUN npm install --omit=dev
COPY server.js ./

ENV NODE_ENV=production
EXPOSE 3000
CMD ["npm", "start"]
