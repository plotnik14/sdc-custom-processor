package org.example.stage.processor.custom;

import ch.hsr.geohash.GeoHash;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.SingleLaneProcessor;
import com.streamsets.pipeline.api.base.SingleLaneRecordProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.example.stage.utils.Constants.*;

public abstract class GeoHashDataProcessor extends SingleLaneRecordProcessor {

    public static final Logger LOGGER = LoggerFactory.getLogger(GeoHashDataProcessor.class);

    /**
     * Gives access to the UI configuration of the stage provided by the {@link GeoHashDataDProcessor} class.
     */
    public abstract String getConfig();

    /** {@inheritDoc} */
    @Override
    protected void process(Record record, SingleLaneProcessor.SingleLaneBatchMaker batchMaker) {
        double latitude = record.get(LATITUDE_FIELD).getValueAsDouble();
        double longitude = record.get(LONGITUDE_FIELD).getValueAsDouble();

        String geoHash = calculateGeoHashString(latitude, longitude);
        record.set(GEOHASH_FIELD, Field.create(geoHash));

        batchMaker.addRecord(record);
    }

    private String calculateGeoHashString(double latitude, double longitude) {
        return GeoHash
                .withCharacterPrecision(latitude, longitude, 4)
                .toBase32();
    }
}
