package org.example.stage.processor.custom;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.StageDef;

@StageDef(
        version = 1,
        label = "LatLong Data DProcessor",
        description =
                "Checks hotel data on incorrect (null) values (Latitude & Longitude). " +
                "For incorrect values maps (Latitude & Longitude) from OpenCage Geocoding API.",
        icon = "default.png",
        onlineHelpRefUrl = ""
)
@ConfigGroups(Groups.class)
@GenerateResourceBundle
public class LatLongDataDProcessor extends LatLongDataProcessor {

    @ConfigDef(
            required = true,
            type = ConfigDef.Type.STRING,
            defaultValue = "default",
            label = "LatLong Config",
            displayPosition = 10,
            group = "HOTEL_DP_GROUP"
    )
    public String config;

    /** {@inheritDoc} */
    @Override
    public String getConfig() {
        return config;
    }

}
