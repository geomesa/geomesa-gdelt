import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.geotools.data.FeatureWriter;
import org.geotools.data.Transaction;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.filter.identity.FeatureIdImpl;
import org.geotools.geometry.jts.JTSFactoryFinder;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: fhp
 * Date: 4/14/14
 * Time: 9:44 AM
 * To change this template use File | Settings | File Templates.
 */
public class IngestToAccumuloMapper extends Mapper<LongWritable,Text,Key,Value> {

    SimpleFeatureType ft = null;
    FeatureWriter<SimpleFeatureType, SimpleFeature> fw = null;
    private SimpleFeatureBuilder featureBuilder;
    private GeometryFactory geometryFactory = JTSFactoryFinder.getGeometryFactory();

    public void setup(Mapper<LongWritable,Text,Key,Value>.Context context) throws IOException, InterruptedException {
        super.setup(context);

        final Map<String , String> connectionParams = new HashMap<String , String>() {{
            put("instanceId", "dcloud");
            put("zookeepers", "dzoo1,dzoo2,dzoo3");
            put("user", "root");
            put("password", "secret");
            put("auths", "S,USA");
            put("tableName", "gdelt_h");
        }};

        String featureName = context.getConfiguration().get(context.getConfiguration().get("featureName"));

        DataStore ds = DataStoreFinder.getDataStore(connectionParams);

        ft = ds.getSchema(featureName);
        featureBuilder = new SimpleFeatureBuilder(ft);
        fw = ds.getFeatureWriter(featureName, Transaction.AUTO_COMMIT);
    }

    public void map(LongWritable key, Text value, Mapper<LongWritable,Text,Key,Value>.Context context) {
        featureBuilder.reset();
        String[] attributes = value.toString().split("\\t");
        featureBuilder.addAll(attributes);
        Double lat = Double.parseDouble(attributes[39]);
        Double lon = Double.parseDouble(attributes[40]);
        Geometry geom = geometryFactory.createPoint(new Coordinate(lon, lat));
        SimpleFeature simpleFeature = featureBuilder.buildFeature(attributes[0]);
        simpleFeature.setDefaultGeometry(geom);
        try {
            SimpleFeature next = fw.next();
            next.setAttributes(simpleFeature.getAttributes());
            ((FeatureIdImpl)next.getIdentifier()).setID(simpleFeature.getID());
        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
    }
}
