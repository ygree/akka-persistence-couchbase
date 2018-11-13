FROM couchbase/server:5.5.1

COPY /configure.sh /

RUN ["chmod", "+x", "/configure.sh"]

ENTRYPOINT ["/configure.sh"]
