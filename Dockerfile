FROM node:22-alpine

WORKDIR /app

COPY package*.json ./
RUN npm install

COPY . .

EXPOSE 8011

CMD ["sh", "./scripts/docker-start.sh"]
