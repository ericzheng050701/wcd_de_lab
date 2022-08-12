/*
 * 
 * 4_wallnd_loaddata.sh
snowsql -c wcd -q "PUT file://calendar.csv @~";
snowsql -c wcd -q "PUT file://store.csv @~";
snowsql -c wcd -q "PUT file://product.csv @~";
snowsql -c wcd -q "PUT file://sales.csv @~";
snowsql -c wcd -q "PUT file://inventory.csv @~";


snowsql -c wcd -q "COPY INTO walmart_dev.wallnd.store FROM @~/store.csv.gz FILE_FORMAT = (TYPE = CSV FIELD_DELIMITER = ',' SKIP_HEADER = 1);";
snowsql -c wcd -q "COPY INTO walmart_dev.wallnd.sales FROM @~/sales.csv.gz FILE_FORMAT = (TYPE = CSV FIELD_DELIMITER = ',' SKIP_HEADER = 1);";
snowsql -c wcd -q "COPY INTO walmart_dev.wallnd.calendar FROM @~/calendar.csv.gz FILE_FORMAT = (TYPE = CSV FIELD_DELIMITER = ',' SKIP_HEADER = 1);";
snowsql -c wcd -q "COPY INTO walmart_dev.wallnd.product FROM @~/product.csv.gz FILE_FORMAT = (TYPE = CSV FIELD_DELIMITER = ',' SKIP_HEADER = 1);";
snowsql -c wcd -q "COPY INTO walmart_dev.wallnd.inventory FROM @~/inventory.csv.gz FILE_FORMAT = (TYPE = CSV FIELD_DELIMITER = ',' SKIP_HEADER = 1);";

snowsql -c wcd -q"list @~";
*/
