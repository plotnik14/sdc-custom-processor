package org.example.stage.processor.custom;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.StageDef;

@StageDef(
        version = 1,
        label = "GeoHash Data Processor",
        description = "Generate geohash by Latitude & Longitude with 4-characters length in extra column.",
        icon = "default.png",
        onlineHelpRefUrl = ""
)
@ConfigGroups(Groups.class)
@GenerateResourceBundle
public class GeoHashDataDProcessor extends GeoHashDataProcessor {

    @ConfigDef(
            required = true,
            type = ConfigDef.Type.STRING,
            defaultValue = "default",
            label = "GeoHash Config",
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
