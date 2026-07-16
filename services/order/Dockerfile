# Build from the repository ROOT:  docker build -f services/order/Dockerfile .
FROM node:24-alpine AS build
WORKDIR /app
COPY package.json package-lock.json tsconfig.json tsconfig.base.json ./
COPY packages ./packages
COPY services ./services
# --ignore-scripts skips optional native builds (better-sqlite3); the code
# falls back to the built-in node:sqlite driver at runtime.
RUN npm ci --ignore-scripts && npm run build

FROM node:24-alpine
WORKDIR /app
ENV NODE_ENV=production SQLITE_DRIVER=node
COPY --from=build /app ./
EXPOSE 3000
CMD ["node", "services/order/dist/main.js"]
