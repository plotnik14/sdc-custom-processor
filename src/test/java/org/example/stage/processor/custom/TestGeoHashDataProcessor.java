package org.example.stage.processor.custom;

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.sdk.ProcessorRunner;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.sdk.StageRunner;
import org.example.stage.utils.Constants;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.Collections;
import java.util.HashMap;

@RunWith(PowerMockRunner.class)
@PrepareForTest(GeoHashDataProcessor.class)
public class TestGeoHashDataProcessor {
    private static final String GEO_HASH_RESULT = "9r2z";

    private Record record;

    @Before
    public void setup() {
        record = RecordCreator.create("s", "s:1");
        record.set(Field.create(new HashMap<String, Field>()));

        record.set(Constants.ID_FIELD, Field.create("8589934594"));
        record.set(Constants.NAME_FIELD, Field.create("Holiday Inn Express"));
        record.set(Constants.COUNTRY_FIELD, Field.create("US"));
        record.set(Constants.CITY_FIELD, Field.create("Ashland"));
        record.set(Constants.ADDRESS_FIELD, Field.create("555 Clover Ln"));
        record.set(Constants.LATITUDE_FIELD, Field.create(42.183544));
        record.set(Constants.LONGITUDE_FIELD, Field.create(-122.663345));
    }
    @Test
    public void testGeoHashGeneration() throws StageException {
        GeoHashDataDProcessor mockClass = PowerMockito.spy(new GeoHashDataDProcessor());

        ProcessorRunner runner = new ProcessorRunner.Builder(GeoHashDataDProcessor.class, mockClass)
                .addConfiguration("config", "value")
                .addOutputLane("output")
                .build();
        runner.runInit();

        try {
            StageRunner.Output output = runner.runProcess(Collections.singletonList(record));
            String geoHash = output.getRecords().get("output").get(0).get(Constants.GEOHASH_FIELD).getValueAsString();
            Assert.assertEquals(GEO_HASH_RESULT, geoHash);
        } finally {
            runner.runDestroy();
        }
    }

}
