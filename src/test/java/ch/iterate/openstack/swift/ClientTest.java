package ch.iterate.openstack.swift;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.http.Header;
import org.junit.Before;
import org.junit.Test;

import ch.iterate.openstack.swift.Client.AuthVersion;
import ch.iterate.openstack.swift.exception.ContainerExistsException;
import ch.iterate.openstack.swift.exception.NotFoundException;
import ch.iterate.openstack.swift.model.ContainerMetadata;
import ch.iterate.openstack.swift.model.MimeType;
import ch.iterate.openstack.swift.model.ObjectMetadata;
import ch.iterate.openstack.swift.model.Region;

public class ClientTest {

	private Client currentClient;
	private URI authUrl = null;
	private URI storageUrl = null;
	private String username = "admin:admin";
	private String password = "iflytek";
	private String tenantId = "admin";

	private AuthenticationResponse authenticationResponse = null;
	private String authtoken = "";

	/**
	 * 初始化相关测试变量
	 * 
	 * @throws IOException
	 */
	@Before
	public void initialize() throws IOException {
		try {
			authUrl = new URI("http://192.168.11.231:7777/auth/v1.0");
			storageUrl = new URI("http://192.168.11.231:7777/v1/AUTH_admin");
		} catch (URISyntaxException e) {
			e.printStackTrace();
		}
		currentClient = new Client(10000);
		try {

			authenticationResponse = currentClient.authenticate(
					AuthVersion.v10, authUrl, username, password, tenantId);
			authtoken = authenticationResponse.getAuthToken();
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
		assertNotNull(authtoken);

		Header urlString = authenticationResponse
				.getResponseHeader(Constants.X_STORAGE_URL);
		assertEquals(storageUrl.toString(), urlString.getValue());
	}

	/**
	 * 测试认证
	 * 
	 * @throws IOException
	 *             访问认证不通过
	 */
	@Test
	public void testAuthenticateAuthVersionURIStringStringString()
			throws IOException {

		authenticationResponse = currentClient.authenticate(AuthVersion.v10,
				authUrl, username, password, tenantId);
		authtoken = authenticationResponse.getAuthToken();

		assertNotNull(authtoken);

		Header urlString = authenticationResponse
				.getResponseHeader(Constants.X_STORAGE_URL);
		assertEquals(storageUrl.toString(), urlString.getValue());

	}

	@Test
	public void testContainerExists() throws IOException {

		String regionId = null;
		Region region = new Region(regionId, storageUrl, null);
		String container = "beyeaudios";
		boolean isExists = currentClient.containerExists(region, container);
		assertTrue(isExists);
	}

	@Test
	public void testCreateContainer() throws IOException {
		String regionId = null;
		Region region = new Region(regionId, storageUrl, null);
		String container = "beyetest";
		try {
			currentClient.createContainer(region, container);
			boolean isExists = currentClient.containerExists(region, container);
			assertTrue(isExists);

		} catch (ContainerExistsException e) {
			// TODO: handle exception
			//container已经存在了
			assertTrue(true);
		}
	}

	@Test
	public void testDeleteContainer() throws IOException {
		String regionId = null;
		Region region = new Region(regionId, storageUrl, null);
		String container = "beyetest";
		currentClient.deleteContainer(region, container);
		boolean isExists = currentClient.containerExists(region, container);
		assertFalse(isExists);
	}

	/**
	 * 
	 * @throws IOException
	 */
	@Test
	public void testStoreObjectRegionStringInputStreamStringStringMapOfStringString()
			throws IOException {
		String regionId = null;
		Region region = new Region(regionId, storageUrl, null);
		String container = "beyedocs";
		String objectnameString = "test_forget.txt";
		InputStream data = new ByteArrayInputStream(new byte[600]);

		String etag = currentClient.storeObject(region, container, data,
				MimeType.txt.contentType, objectnameString,
				new HashMap<String, String>());
		assertNotNull(etag);
	}

	/**
	 * 
	 * @throws IOException
	 */
	@Test(expected = NotFoundException.class)
	public void testDeleteObject() throws IOException {
		String regionId = null;
		Region region = new Region(regionId, storageUrl, null);
		String container = "beyetest1";
		String objName = "test.txt";
		InputStream data = new ByteArrayInputStream(new byte[200]);
		currentClient.storeObject(region, container, data,
				MimeType.txt.contentType, objName,
				new HashMap<String, String>());
		currentClient.deleteObject(region, container, objName);
		currentClient.getObjectMetaData(region, container, objName);
	}

	@Test
	public void testDeleteObjects() throws IOException {
		String regionId = null;
		Region region = new Region(regionId, storageUrl, null);
		String container = "beyetest1";
		boolean isexists = currentClient.containerExists(region, container);
		if (!isexists) {
			currentClient.createContainer(region, container);
		}
		ArrayList<String> objectNames = new ArrayList<String>();

		InputStream data = new ByteArrayInputStream(new byte[200]);
		for (int i = 0; i < 3; i++) {
			String objName = "test" + i + ".txt";
			objectNames.add(objName);
			currentClient.storeObject(region, container, data,
					MimeType.txt.contentType, objName,
					new HashMap<String, String>());
		}

		currentClient.deleteObjects(region, container, objectNames);

		int deletecount = 0;
		for (String objname : objectNames) {
			try {
				currentClient.getObjectMetaData(region, container, objname);
			} catch (NotFoundException e) {
				deletecount = deletecount + 1;
			}
		}
		assertEquals(3, deletecount);
	}

	@Test
	public void testGetObjectRegionStringString() throws IOException {
		String regionId = null;
		Region region = new Region(regionId, storageUrl, null);
		String container = "beyedocs";
		String objname = "test_forget.txt";
		InputStream data = currentClient.getObject(region, container, objname);

		assertEquals(600, data.skip(600));
	}

	// @Test
	// public void testGetObjectRegionStringStringLongLong() throws IOException
	// {
	// String regionId = null;
	// Region region = new Region(regionId, storageUrl, null);
	// String container = "beyedocs";
	// String objname = "test_forget.txt";
	//
	// InputStream data = currentClient.getObject(region, container, objname,
	// 0, 200);
	//
	// assertEquals(200, data.skip(200));
	// }

	@Test
	public void testUpdateObjectMetadata() throws IOException {
		String regionId = null;
		Region region = new Region(regionId, storageUrl, null);
		String container = "beyedocs";
		String objname = "test_forget.txt";
		Map<String, String> metadata = new HashMap<String, String>();
		metadata.put("Marker", "mlchen");

		currentClient
				.updateObjectMetadata(region, container, objname, metadata);

		ObjectMetadata objectMetadata = currentClient.getObjectMetaData(region,
				container, objname);

		Map<String, String> existsMetadataMap = objectMetadata.getMetaData();
		assertTrue(existsMetadataMap.containsKey(Constants.X_OBJECT_META
				+ "Marker"));
	}

	@Test
	public void testUpdateContainerMetadata() throws IOException {
		String regionId = null;
		Region region = new Region(regionId, storageUrl, null);
		String container = "beyetest";
		boolean isexists = currentClient.containerExists(region, container);
		if (!isexists) {
			currentClient.createContainer(region, container);
		}
		Map<String, String> metadata = new HashMap<String, String>();
		metadata.put("Marker", "mlchen");
		currentClient.updateContainerMetadata(region, container, metadata);

		ContainerMetadata containerMetadata = currentClient
				.getContainerMetaData(region, container);

		Map<String, String> existsMetadataMap = containerMetadata.getMetaData();
		assertTrue(existsMetadataMap.containsKey(Constants.X_CONTAINER_META
				+ "Marker"));
	}

	// @Test
	// public void testUpdateAccountMetadata() throws IOException {
	// String regionId = null;
	// Region region = new Region(regionId, storageUrl, null);
	//
	// Map<String, String> metadata = new HashMap<String, String>();
	// metadata.put("Marker", "mlchen");
	// currentClient.updateAccountMetadata(region, metadata);
	//
	// assertTrue(true);
	//
	// }

}
