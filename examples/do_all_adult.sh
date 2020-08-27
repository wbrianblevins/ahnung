
export OMP_NUM_THREADS=1


# ./run_pipeline.py ./examples/config_ahnung_adult_t225.json  model

# w; sensors; date
# sleep 120
# w; sensors; date
# date

# ./run_pipeline.py ./examples/config_ahnung_adult_t450.json  model

# w; sensors; date
# date
# sleep 120
# w; sensors; date
# date

./run_pipeline.py ./examples/config_ahnung_adult_t900.json  model

w; sensors; date
date
sleep 120
w; sensors; date
date

./run_pipeline.py ./examples/config_ahnung_adult_t1800.json  model

w; sensors; date
date
sleep 120
w; sensors; date
date

./run_pipeline.py ./examples/config_ahnung_adult_t3600.json  model

w; sensors; date
date
sleep 120
w; sensors; date
date

./run_pipeline.py ./examples/config_ahnung_adult_t7200.json  model



