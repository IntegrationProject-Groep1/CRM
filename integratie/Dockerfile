FROM node:22-slim

ENV NODE_ENV=production

WORKDIR /app

COPY package.json .
RUN npm install --omit=dev

COPY . .

CMD ["node", "mcp_server.js"]
