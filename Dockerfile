#Latest version of node tested on.
FROM node:8-alpine

WORKDIR /app

# Only run npm install if these files change.
ADD ./package.json /app/package.json

# Install dependencies
RUN npm install

# Add the rest of the sources
ADD . /app

EXPOSE 3000

CMD ["npm","start"]