import geomesa.core.index.Constants;
import org.apache.hadoop.fs.Path;
import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.geotools.data.DataUtilities;
import org.opengis.feature.simple.SimpleFeatureType;
import java.util.HashMap;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: fhp
 * Date: 4/11/14
 * Time: 3:20 PM
 * To change this template use File | Settings | File Templates.
 */
public class MapReduceIngest {
    static String DEFAULT_FEATURE_NAME = "geomesa.index.feature";

    public static void main(String [ ] args) throws Exception {
        final String zookeepers = "dzoo1,dzoo2,dzoo3";

        /*SimpleFeatureTypeBuilder sftb = new SimpleFeatureTypeBuilder();

        sftb.setCRS(DefaultGeographicCRS.WGS84);
        sftb.setSRS("EPSG:4326");
        sftb.setName("GDELT");
        sftb.add("geomesa_index_geometry", Geometry.class);
        sftb.add("geomesa_index_start_time", Date.class);
        sftb.add("geomesa_index_end_time", Date.class);


        SimpleFeature sft = sftb.buildFeatureType();*/


        final Map<String , String> dsConf = new HashMap<String , String>() {{
            put("instanceId", "dcloud");
            put("zookeepers", zookeepers);
            put("user", "root");
            put("password", "secret");
            put("auths", "S,USA");
            put("tableName", "gdelt_h");
        }};

        DataStore ds = DataStoreFinder.getDataStore(dsConf);


        String name = "gdelt";
        String sftSpec =
        "GLOBALEVENTID:Integer,SQLDATE:Date,MonthYear:Integer,Year:Integer,FractionDate:Float,Actor1Code:String,Actor1Name:String,Actor1CountryCode:String,Actor1KnownGroupCode:String,Actor1EthnicCode:String,Actor1Religion1Code:String,Actor1Religion2Code:String,Actor1Type1Code:String,Actor1Type2Code:String,Actor1Type3Code:String,Actor2Code:String,Actor2Name:String,Actor2CountryCode:String,Actor2KnownGroupCode:String,Actor2EthnicCode:String,Actor2Religion1Code:String,Actor2Religion2Code:String,Actor2Type1Code:String,Actor2Type2Code:String,Actor2Type3Code:String,IsRootEvent:Integer,EventCode:String,EventBaseCode:String,EventRootCode:String,QuadClass:Integer,GoldsteinScale:Float,NumMentions:Integer,NumSources:Integer,NumArticles:Integer,AvgTone:Float,Actor1Geo_Type:Integer,Actor1Geo_FullName:String,Actor1Geo_CountryCode:String,Actor1Geo_ADM1Code:String,Actor1Geo_Lat:Float,Actor1Geo_Long:Float,Actor1Geo_FeatureID:Integer,Actor2Geo_Type:Integer,Actor2Geo_FullName:String,Actor2Geo_CountryCode:String,Actor2Geo_ADM1Code:String,Actor2Geo_Lat:Float,Actor2Geo_Long:Float,Actor2Geo_FeatureID:Integer,ActionGeo_Type:Integer,ActionGeo_FullName:String,ActionGeo_CountryCode:String,ActionGeo_ADM1Code:String,ActionGeo_Lat:Float,ActionGeo_Long:Float,ActionGeo_FeatureID:Integer,DATEADDED:Integer,*geom:Point:srid=4326";

        SimpleFeatureType featureType = DataUtilities.createType(name, sftSpec);
        featureType.getUserData().put(Constants.SF_PROPERTY_START_TIME, "SQLDATE");
        ds.createSchema(featureType);


        MapReduceJobRunner runner = new MapReduceJobRunner();
        runner.runMapReduceJob(name, new Path("hdfs:///gdelt/compressed/gdelt.tsv"), "hdfs:///tmp/hunter/ingestJar/original-MapReduceIngest-1.0-SNAPSHOT.jar");
        // run map-reduce job with tsv in hdfs as input
    }
}
