FROM node:22.22.0-bookworm-slim@sha256:dd9d21971ec4395903fa6143c2b9267d048ae01ca6d3ea96f16cb30df6187d94
WORKDIR /app
COPY frontend/package*.json ./
RUN npm ci
COPY frontend .
RUN npm run build
RUN mkdir -p node_modules/.vite-temp && chown -R node:node node_modules/.vite-temp
USER node
EXPOSE 5173
CMD ["npm","run","preview"]
