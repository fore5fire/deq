FROM scratch

ENV PATH /bin
COPY ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY deqd /bin/deqd

EXPOSE 8080/tcp

ENTRYPOINT ["/bin/deqd"]
