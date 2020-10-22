FROM erlang:21

# Set working directory
RUN mkdir /fmke_populator
WORKDIR /fmke_populator

# Copy FMKe application
COPY . .
RUN chmod +x run_fmke_pop.sh

#build 
RUN  rebar3 escriptize

ENTRYPOINT ["/fmke_populator/run_fmke_pop.sh"]
