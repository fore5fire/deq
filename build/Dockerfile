FROM scratch

ENV PATH /bin
COPY ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY deqd /bin/deqd
COPY deqctl /bin/deqctl
ENTRYPOINT ["/bin/deqd"]
