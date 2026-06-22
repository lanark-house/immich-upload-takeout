# Stage 1: Build
FROM node:24-alpine AS builder

WORKDIR /app

COPY package*.json ./
RUN npm ci --omit=dev

# Stage 2: Runtime
FROM node:24-alpine

WORKDIR /app

# Create tmp directory and set permissions
RUN mkdir -p /work/tmp && chown -R node:node /work/tmp

COPY --from=builder /app/node_modules ./node_modules
COPY . .

USER node

EXPOSE 3000

CMD ["node", "index.js"]
