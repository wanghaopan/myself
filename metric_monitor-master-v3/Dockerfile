FROM  10.168.25.194:5000/transwarp/metrics-monitor-build:v1
RUN rm -rf /opt/metrics-monitor &&  mkdir -p /opt/metrics-monitor
ADD . /opt/metrics-monitor
RUN rm -f /opt/metrics-monitor/Config.py