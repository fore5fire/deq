FROM scratch

COPY ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY deqd /bin/deqd
ENTRYPOINT ["/bin/deqd"]
