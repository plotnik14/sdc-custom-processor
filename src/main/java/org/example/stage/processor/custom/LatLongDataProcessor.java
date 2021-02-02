package org.example.stage.processor.custom;

import com.byteowls.jopencage.JOpenCageGeocoder;
import com.byteowls.jopencage.model.JOpenCageForwardRequest;
import com.byteowls.jopencage.model.JOpenCageLatLng;
import com.byteowls.jopencage.model.JOpenCageResponse;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.base.SingleLaneProcessor;
import com.streamsets.pipeline.api.base.SingleLaneRecordProcessor;
import org.example.stage.utils.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.example.stage.utils.Constants.*;

public abstract class LatLongDataProcessor extends SingleLaneRecordProcessor {

    public static final Logger LOGGER = LoggerFactory.getLogger(LatLongDataProcessor.class);

    /**
     * Gives access to the UI configuration of the stage provided by the {@link LatLongDataDProcessor} class.
     */
    public abstract String getConfig();

    /** {@inheritDoc} */
    @Override
    protected void process(Record record, SingleLaneProcessor.SingleLaneBatchMaker batchMaker) {
        String latitude = record.get(LATITUDE_FIELD).getValueAsString();
        String longitude = record.get(LONGITUDE_FIELD).getValueAsString();

        if (isCoordinateValid(latitude) && isCoordinateValid(longitude)) {
            batchMaker.addRecord(record);
            return;
        }

        String name = record.get(NAME_FIELD).getValueAsString();
        String country = record.get(COUNTRY_FIELD).getValueAsString();
        String city = record.get(CITY_FIELD).getValueAsString();
        String address = record.get(ADDRESS_FIELD).getValueAsString();

        String searchString = new StringBuilder()
                .append(name).append(DELIMITER)
                .append(address).append(DELIMITER)
                .append(city).append(DELIMITER)
                .append(country).toString();

        JOpenCageLatLng jOpenCageLatLng = getCoordinates(searchString);
        if (jOpenCageLatLng == null) {
            LOGGER.warn("No coordinates were found for searchString:{}", searchString);
            batchMaker.addRecord(record);
            return;
        }

        Double newLatitude = jOpenCageLatLng.getLat();
        Double newLongitude = jOpenCageLatLng.getLng();

        record.set(LATITUDE_FIELD, Field.create(newLatitude));
        record.set(LONGITUDE_FIELD, Field.create(newLongitude));

        batchMaker.addRecord(record);
    }

    private JOpenCageLatLng getCoordinates(String searchString) {
//        if (searchString == null || searchString.isEmpty()) {
//            throw new IllegalArgumentException("searchString:" + searchString);
//        }

        JOpenCageGeocoder jOpenCageGeocoder = new JOpenCageGeocoder(Constants.OPEN_CAGE_GEOCODER_API_KEY);
        JOpenCageForwardRequest request = new JOpenCageForwardRequest(searchString);
        JOpenCageResponse response = jOpenCageGeocoder.forward(request);
        return response.getFirstPosition();
    }

    private boolean isCoordinateValid(String coordinate) {
        return coordinate != null
                && !coordinate.isEmpty()
                && !"NA".equals(coordinate);
    }
}
