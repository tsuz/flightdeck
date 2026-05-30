FROM node:20-alpine AS build
WORKDIR /app
COPY package.json package-lock.json* ./
RUN npm install
COPY . .
RUN npm run build

FROM nginx:alpine
# envsubst (from gettext) renders runtime config at container start.
RUN apk add --no-cache gettext
COPY --from=build /app/dist /usr/share/nginx/html
COPY nginx.conf /etc/nginx/conf.d/default.conf
COPY env.template.js /etc/flightdeck/env.template.js
COPY docker-entrypoint.sh /docker-entrypoint.d/40-flightdeck-env.sh
RUN chmod +x /docker-entrypoint.d/40-flightdeck-env.sh
EXPOSE 80
