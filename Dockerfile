FROM node:22-alpine

WORKDIR /app

# Install build dependencies for libxmljs2
RUN apk add --no-cache libxml2-dev \
    && apk add --no-cache --virtual .build-deps \
        python3 \
        make \
        g++ \
    && COPY package*.json ./ \
    && npm ci --only=production \
    && apk del .build-deps

COPY src/ ./src/

RUN chown -R node:node /app
USER node

HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
  CMD wget -qO- http://127.0.0.1:3000/health || exit 1

CMD ["node", "src/receiver.js"]
