package mobisocial.cocoon.test;

import java.io.IOException;

import junit.framework.TestCase;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.BasicHttpEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

/**
 * Unit test for simple App.
 */
public class RegistrationTest extends TestCase {
    //public static final String MONARCH_AUTHORITY = "http://monarch.musubi.us:6253";
    public static final String MONARCH_AUTHORITY = "http://localhost:6253";
    public void testRegistration() throws IOException, JSONException {
        HttpClient http = new DefaultHttpClient();
        HttpPost post = new HttpPost(MONARCH_AUTHORITY + "/api/0/register");

        JSONObject request = new JSONObject();
        JSONArray identities = new JSONArray();
        identities.put("ident-1")
            .put("ident-2");
        request.put("identityExchanges", identities);
        request.put("deviceToken", Base64.encodeBase64String(new byte[] {0x7f,0x7e,0x7d}));
        BasicHttpEntity entity = new BasicHttpEntity();
        entity.setContentType("application/json");
        entity.setContent(IOUtils.toInputStream(request.toString()));

        post.setEntity(entity);
        HttpResponse response = http.execute(post);
        String result = IOUtils.toString(response.getEntity().getContent());
        assertEquals(result, "ok");
    }
}
