FROM cubejs/cube:jdk
USER root
RUN apt-get update
RUN apt-get install build-essential -y
ADD ./cube-js-mongo-atlas-driver /tmp/packages/cube-js-mongo-atlas-driver
RUN yarn add file:/tmp/packages/cube-js-mongo-atlas-driver