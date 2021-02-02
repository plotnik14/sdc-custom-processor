package org.example.stage.processor.custom;


import com.byteowls.jopencage.model.JOpenCageLatLng;
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

import static org.mockito.ArgumentMatchers.any;

@RunWith(PowerMockRunner.class)
@PrepareForTest(LatLongDataProcessor.class)
public class TestLatLongDataProcessor {
    private static final double LATITUDE_TEST_VALUE = 42.183544;
    private static final double LONGITUDE_TEST_VALUE = -122.663345;

    private Record recordWithLatLng;
    private Record recordWithoutLatLng;

    @Before
    public void setup() {
        recordWithLatLng = RecordCreator.create("s", "s:1");
        recordWithLatLng.set(Field.create(new HashMap<String, Field>()));

        recordWithLatLng.set(Constants.ID_FIELD, Field.create("8589934594"));
        recordWithLatLng.set(Constants.NAME_FIELD, Field.create("Holiday Inn Express"));
        recordWithLatLng.set(Constants.COUNTRY_FIELD, Field.create("US"));
        recordWithLatLng.set(Constants.CITY_FIELD, Field.create("Ashland"));
        recordWithLatLng.set(Constants.ADDRESS_FIELD, Field.create("555 Clover Ln"));
        recordWithLatLng.set(Constants.LATITUDE_FIELD, Field.create(LATITUDE_TEST_VALUE));
        recordWithLatLng.set(Constants.LONGITUDE_FIELD, Field.create(LONGITUDE_TEST_VALUE));

        // ----------------------

        recordWithoutLatLng = RecordCreator.create("s", "s:1");
        recordWithoutLatLng.set(Field.create(new HashMap<String, Field>()));

        recordWithoutLatLng.set(Constants.ID_FIELD, Field.create("8589934594"));
        recordWithoutLatLng.set(Constants.NAME_FIELD, Field.create("Holiday Inn Express"));
        recordWithoutLatLng.set(Constants.COUNTRY_FIELD, Field.create("US"));
        recordWithoutLatLng.set(Constants.CITY_FIELD, Field.create("Ashland"));
        recordWithoutLatLng.set(Constants.ADDRESS_FIELD, Field.create("555 Clover Ln"));
        recordWithoutLatLng.set(Constants.LATITUDE_FIELD, Field.create(""));
        recordWithoutLatLng.set(Constants.LONGITUDE_FIELD, Field.create(""));
    }

    @Test
    public void testCorrectLatLngCase() throws StageException {
        LatLongDataDProcessor mockClass = PowerMockito.spy(new LatLongDataDProcessor());

        ProcessorRunner runner = new ProcessorRunner.Builder(LatLongDataDProcessor.class, mockClass)
                .addConfiguration("config", "value")
                .addOutputLane("output")
                .build();
        runner.runInit();

        try {
            StageRunner.Output output = runner.runProcess(Collections.singletonList(recordWithLatLng));
            double latitude = output.getRecords().get("output").get(0).get(Constants.LATITUDE_FIELD).getValueAsDouble();
            double longitude = output.getRecords().get("output").get(0).get(Constants.LONGITUDE_FIELD).getValueAsDouble();
            Assert.assertEquals(LATITUDE_TEST_VALUE, latitude, 0.0);
            Assert.assertEquals(LONGITUDE_TEST_VALUE, longitude, 0.0);
        } finally {
            runner.runDestroy();
        }
    }

    @Test
    public void testMissingLatLngCase() throws Exception {
        LatLongDataDProcessor mockClass = PowerMockito.spy(new LatLongDataDProcessor());
        PowerMockito.doReturn(createCoordinates()).when(mockClass, "getCoordinates", any(String.class));

        ProcessorRunner runner = new ProcessorRunner.Builder(LatLongDataDProcessor.class, mockClass)
                .addConfiguration("config", "value")
                .addOutputLane("output")
                .build();
        runner.runInit();

        try {
            StageRunner.Output output = runner.runProcess(Collections.singletonList(recordWithoutLatLng));
            double latitude = output.getRecords().get("output").get(0).get(Constants.LATITUDE_FIELD).getValueAsDouble();
            double longitude = output.getRecords().get("output").get(0).get(Constants.LONGITUDE_FIELD).getValueAsDouble();
            Assert.assertEquals(LATITUDE_TEST_VALUE, latitude, 0.0);
            Assert.assertEquals(LONGITUDE_TEST_VALUE, longitude, 0.0);
        } finally {
            runner.runDestroy();
        }
    }

    private JOpenCageLatLng createCoordinates() {
        JOpenCageLatLng jOpenCageLatLng = new JOpenCageLatLng();
        jOpenCageLatLng.setLat(LATITUDE_TEST_VALUE);
        jOpenCageLatLng.setLng(LONGITUDE_TEST_VALUE);
        return jOpenCageLatLng;
    }
}
