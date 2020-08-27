#!/bin/bash

ECHO="echo -e"
CURL="curl --insecure"

MONGOIMPORT="mongoimport --drop"

EJ_DBL_B='{"$numberDouble": "'
EJ_DBL_E='" }'

EJ_INT32_B='{"$numberInt": "'
EJ_INT32_E='" }'

EJ_INT64_B='{"$numberLong": "'
EJ_INT64_E='" }'

UCIML_DB="UCI-ML"

#
# Put in the connection string URI for your data source replica set.
# Note: do not specify any database in the URI.
#
REPL_DATA_USER="notmyuser:notmypassword"
REPL_ATLAS_CLUSTER="wbbdata.wipjb.mongodb.net"
# REPL_DATA_SRC="mongodb+srv://$REPL_DATA_USER@$REPL_ATLAS_CLUSTER/$UCIML_DB?&authSource=admin&w=majority&wtimeoutMS=5000"
REPL_DATA_SRC="mongodb+srv://$REPL_DATA_USER@$REPL_ATLAS_CLUSTER/$UCIML_DB?&authSource=admin&w=majority"
# REPL_DATA_SRC="mongodb://127.0.0.1/$UCIML_DB?&w=majority"


convert_iris_csv2json() {

    SRC_CSV="$1"
    shift
    OUT_JSON="$1"
    shift

    F_NAMES=$*

    N1="$1"
    shift
    N2="$1"
    shift
    N3="$1"
    shift
    N4="$1"
    shift
    N5="$1"
    shift
    
    # echo $SRC_CSV
    # echo $OUT_JSON
    # echo $F_NAMES

    while IFS=, read -r f1 f2 f3 f4 f5
    do
        if [ "" != "$f1" ]
        then
            $ECHO '{ ' >> "$OUT_JSON"
            $ECHO '    "'$N1'": '$EJ_DBL_B$f1$EJ_DBL_E',' >> "$OUT_JSON"
            $ECHO '    "'$N2'": '$EJ_DBL_B$f2$EJ_DBL_E',' >> "$OUT_JSON"
            $ECHO '    "'$N3'": '$EJ_DBL_B$f3$EJ_DBL_E',' >> "$OUT_JSON"
            $ECHO '    "'$N4'": '$EJ_DBL_B$f4$EJ_DBL_E',' >> "$OUT_JSON"
            $ECHO '    "'$N5'": "'$f5'"' >> "$OUT_JSON"
            $ECHO '}' >> "$OUT_JSON"
        fi
    done < "$SRC_CSV"
}


convert_wine_csv2json() {

    SRC_CSV="$1"
    shift
    OUT_JSON="$1"
    shift

    F_NAMES=$*

    N01="$1"
    shift
    N02="$1"
    shift
    N03="$1"
    shift
    N04="$1"
    shift
    N05="$1"
    shift
    N06="$1"
    shift
    N07="$1"
    shift
    N08="$1"
    shift
    N09="$1"
    shift
    N10="$1"
    shift
    N11="$1"
    shift
    N12="$1"
    shift
    N13="$1"
    shift
    N14="$1"
    shift
    
    # echo $SRC_CSV
    # echo $OUT_JSON
    # echo $F_NAMES

    while IFS=, read -r f01 f02 f03 f04 f05 f06 f07 f08 f09 f10 f11 f12 f13 f14
    do
        if [ "" != "$f01" ]
        then
            $ECHO '{ ' >> "$OUT_JSON"
            $ECHO '    "'$N01'": '$EJ_INT32_B$f01$EJ_INT32_E',' >> "$OUT_JSON"
            $ECHO '    "'$N02'": '$EJ_DBL_B$f02$EJ_DBL_E',' >> "$OUT_JSON"
            $ECHO '    "'$N03'": '$EJ_DBL_B$f03$EJ_DBL_E',' >> "$OUT_JSON"
            $ECHO '    "'$N04'": '$EJ_DBL_B$f04$EJ_DBL_E',' >> "$OUT_JSON"
            $ECHO '    "'$N05'": '$EJ_DBL_B$f05$EJ_DBL_E',' >> "$OUT_JSON"
            $ECHO '    "'$N06'": '$EJ_INT32_B$f06$EJ_INT32_E',' >> "$OUT_JSON"
            $ECHO '    "'$N07'": '$EJ_DBL_B$f07$EJ_DBL_E',' >> "$OUT_JSON"
            $ECHO '    "'$N08'": '$EJ_DBL_B$f08$EJ_DBL_E',' >> "$OUT_JSON"
            $ECHO '    "'$N09'": '$EJ_DBL_B$f09$EJ_DBL_E',' >> "$OUT_JSON"
            $ECHO '    "'$N10'": '$EJ_DBL_B$f10$EJ_DBL_E',' >> "$OUT_JSON"
            $ECHO '    "'$N11'": '$EJ_DBL_B$f11$EJ_DBL_E',' >> "$OUT_JSON"
            $ECHO '    "'$N12'": '$EJ_DBL_B$f12$EJ_DBL_E',' >> "$OUT_JSON"
            $ECHO '    "'$N13'": '$EJ_DBL_B$f13$EJ_DBL_E',' >> "$OUT_JSON"
            $ECHO '    "'$N14'": '$EJ_INT32_B$f14$EJ_INT32_E >> "$OUT_JSON"
            $ECHO '}' >> "$OUT_JSON"
        fi
    done < "$SRC_CSV"
}



convert_adult_csv2json() {

    SRC_CSV="$1"
    shift
    OUT_JSON="$1"
    shift

    F_NAMES=$*

    N01="$1"
    shift
    N02="$1"
    shift
    N03="$1"
    shift
    N04="$1"
    shift
    N05="$1"
    shift
    N06="$1"
    shift
    N07="$1"
    shift
    N08="$1"
    shift
    N09="$1"
    shift
    N10="$1"
    shift
    N11="$1"
    shift
    N12="$1"
    shift
    N13="$1"
    shift
    N14="$1"
    shift
    N15="$1"
    shift
    
    # echo $SRC_CSV
    # echo $OUT_JSON
    # echo $F_NAMES

    while IFS=', ' read -r f01 f02 f03 f04 f05 f06 f07 f08 f09 f10 f11 f12 f13 f14 f15
    do
        if [ "" != "$f01" ]
        then
            $ECHO '{ ' >> "$OUT_JSON"
            $ECHO '    "'$N01'": '$EJ_INT32_B$f01$EJ_INT32_E',' >> "$OUT_JSON"
            $ECHO '    "'$N02'": "'$f02'",' >> "$OUT_JSON"
            $ECHO '    "'$N03'": '$EJ_DBL_B$f03$EJ_DBL_E',' >> "$OUT_JSON"
            $ECHO '    "'$N04'": "'$f04'",' >> "$OUT_JSON"
            $ECHO '    "'$N05'": '$EJ_DBL_B$f05$EJ_DBL_E',' >> "$OUT_JSON"
            $ECHO '    "'$N06'": "'$f06'",' >> "$OUT_JSON"
            $ECHO '    "'$N07'": "'$f07'",' >> "$OUT_JSON"
            $ECHO '    "'$N08'": "'$f08'",' >> "$OUT_JSON"
            $ECHO '    "'$N09'": "'$f09'",' >> "$OUT_JSON"
            $ECHO '    "'$N10'": "'$f10'",' >> "$OUT_JSON"
            $ECHO '    "'$N11'": '$EJ_DBL_B$f11$EJ_DBL_E',' >> "$OUT_JSON"
            $ECHO '    "'$N12'": '$EJ_DBL_B$f12$EJ_DBL_E',' >> "$OUT_JSON"
            $ECHO '    "'$N13'": '$EJ_DBL_B$f13$EJ_DBL_E',' >> "$OUT_JSON"
            $ECHO '    "'$N14'": "'$f14'",' >> "$OUT_JSON"
            $ECHO '    "'$N15'": "'$f15'"' >> "$OUT_JSON"
            $ECHO '}' >> "$OUT_JSON"
        fi
    done < "$SRC_CSV"
}


IRIS_DIR="iris"
IRIS_DATA="iris.data"
IRIS_URL_DATA="https://archive.ics.uci.edu/ml/machine-learning-databases/iris/iris.data"

SRC_COLL_IRIS="iris"

stage_iris_dataset() {
    L_PATH="./$IRIS_DIR/$IRIS_DATA"
    L_JSON="./$IRIS_DIR/$IRIS_DATA.json"
    mkdir -p "./$IRIS_DIR"

    if [ ! -e "$L_PATH" ]
    then
        $CURL --output "$L_PATH" "$IRIS_URL_DATA"
        if [ $? -ne 0 ]
        then
            $ECHO "\nUnable to download : $IRIS_URL\n\nExiting...\n"
            exit
        fi
    fi

    if [ ! -e "$L_JSON" ]
    then
        $ECHO "\nConverting iris dataset CSV to JSON for import...\n"
        convert_iris_csv2json "$L_PATH" "$L_JSON" sepal_length sepal_width petal_length petal_width class
    fi

    if [ ! -e "$L_JSON" ]
    then
        $ECHO "\nUnable to covert to JSON : $L_PATH\n\nExiting...\n"
        exit
    else
        $ECHO "\nIngesting: $UCIML_DB:$SRC_COLL_IRIS"
        $ECHO "\nPlease enter password when prompted...\n"
        $MONGOIMPORT --type=json --uri="$REPL_DATA_SRC" --collection=$SRC_COLL_IRIS $L_JSON
    fi

}




WINE_DIR="wine"
WINE_DATA="wine.data"
WINE_URL_DATA="https://archive.ics.uci.edu/ml/machine-learning-databases/wine/wine.data"

SRC_COLL_WINE="wine"

stage_wine_dataset() {
    L_PATH="./$WINE_DIR/$WINE_DATA"
    L_JSON="./$WINE_DIR/$WINE_DATA.json"
    mkdir -p "./$WINE_DIR"

    if [ ! -e "$L_PATH" ]
    then
        $CURL --output "$L_PATH" "$WINE_URL_DATA"
        if [ $? -ne 0 ]
        then
            $ECHO "\nUnable to download : $WINE_URL\n\nExiting...\n"
            exit
        fi
    fi

    if [ ! -e "$L_JSON" ]
    then
        $ECHO "\nConverting wine dataset CSV to JSON for import...\n"
        convert_wine_csv2json "$L_PATH" "$L_JSON" class alcohol malic_acid ash alcalinity_ash magnesium total_phenols flavanoids nf_phenols proanthocyanins color_intensity hue od_ratio proline
    fi

    if [ ! -e "$L_JSON" ]
    then
        $ECHO "\nUnable to covert to JSON : $L_PATH\n\nExiting...\n"
        exit
    else
        $ECHO "\nIngesting: $UCIML_DB:$SRC_COLL_WINE"
        $ECHO "\nPlease enter password when prompted...\n"
        $MONGOIMPORT --type=json --uri="$REPL_DATA_SRC" --collection=$SRC_COLL_WINE $L_JSON
    fi

}





ADULT_DIR="adult"
ADULT_DATA="adult.data"
ADULT_URL_DATA="https://archive.ics.uci.edu/ml/machine-learning-databases/adult/adult.data"

SRC_COLL_ADULT="adult"
# SRC_COLL_ADULT="adult_225"
# SRC_COLL_ADULT="adult_450"
# SRC_COLL_ADULT="adult_900"
# SRC_COLL_ADULT="adult_1800"
# SRC_COLL_ADULT="adult_3600"
# SRC_COLL_ADULT="adult_7200"

stage_adult_dataset() {
    L_PATH="./$ADULT_DIR/$ADULT_DATA"
    L_JSON="./$ADULT_DIR/$ADULT_DATA.json"
    mkdir -p "./$ADULT_DIR"

    if [ ! -e "$L_PATH" ]
    then
        $CURL --output "$L_PATH" "$ADULT_URL_DATA"
        if [ $? -ne 0 ]
        then
            $ECHO "\nUnable to download : $ADULT_URL\n\nExiting...\n"
            exit
        fi
    fi

    if [ ! -e "$L_JSON" ]
    then
        $ECHO "\nConverting adult dataset CSV to JSON for import...\n"
        convert_adult_csv2json "$L_PATH" "$L_JSON" age workclass fnlwgt education ed_num_yrs marital_status occupation relationship race sex cap_gain cap_loss hours_per_week native_country income_class
    fi

    if [ ! -e "$L_JSON" ]
    then
        $ECHO "\nUnable to covert to JSON : $L_PATH\n\nExiting...\n"
        exit
    else
        $ECHO "\nIngesting: $UCIML_DB:$SRC_COLL_ADULT"
        $ECHO "\nPlease enter password when prompted...\n"
        $MONGOIMPORT --type=json --uri="$REPL_DATA_SRC" --collection=$SRC_COLL_ADULT $L_JSON
    fi

}


stage_iris_dataset
stage_wine_dataset
# stage_adult_dataset

