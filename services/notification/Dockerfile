# Build from the repository ROOT:  docker build -f services/notification/Dockerfile .
FROM node:24-alpine AS build
WORKDIR /app
COPY package.json package-lock.json tsconfig.json tsconfig.base.json ./
COPY packages ./packages
COPY services ./services
RUN npm ci --ignore-scripts && npm run build

FROM node:24-alpine
WORKDIR /app
ENV NODE_ENV=production SQLITE_DRIVER=node
COPY --from=build /app ./
CMD ["node", "services/notification/dist/main.js"]
