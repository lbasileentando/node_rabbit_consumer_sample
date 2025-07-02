FROM node:20

WORKDIR /app

COPY package.json package-lock.json ./

RUN npm install

COPY . .

ENV NODE_ENV=production

EXPOSE 5000

CMD ["npm", "start"]
