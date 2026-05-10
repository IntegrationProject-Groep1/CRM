# Stage 1: Build stage
FROM node:22-alpine AS builder

WORKDIR /app

# Install build dependencies for libxmljs2
RUN apk add --no-cache \
    python3 \
    make \
    g++ \
    libxml2-dev

COPY package*.json ./
RUN npm ci --only=production

# Stage 2: Production stage
FROM node:22-alpine

WORKDIR /app

# Install runtime dependency for libxmljs2
RUN apk add --no-cache libxml2

# Copy production node_modules from builder
COPY --from=builder /app/node_modules ./node_modules
COPY package*.json ./
COPY src/ ./src/

RUN chown -R node:node /app
USER node

HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
  CMD wget -qO- http://127.0.0.1:3000/health || exit 1

CMD ["node", "src/receiver.js"]
