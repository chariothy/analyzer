# For spiders project
# @version 1.0

FROM chariothy/pydata:latest
LABEL maintainer="chariothy@gmail.com"

WORKDIR /app
COPY ./requirements/core.txt .

RUN ln -sf /usr/share/zoneinfo/Asia/Shanghai /etc/localtime \
  && echo 'Asia/Shanghai' > /etc/timezone \
  && pip install -U pip \
  && pip install --no-cache-dir -r ./core.txt

CMD [ "python" ]