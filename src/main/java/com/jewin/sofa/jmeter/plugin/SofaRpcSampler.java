package com.jewin.sofa.jmeter.plugin;

import com.alipay.sofa.common.utils.StringUtil;
import com.alipay.sofa.rpc.api.GenericService;
import com.alipay.sofa.rpc.config.ApplicationConfig;
import com.alipay.sofa.rpc.config.ConsumerConfig;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.jmeter.config.Arguments;
import org.apache.jmeter.protocol.java.sampler.AbstractJavaSamplerClient;
import org.apache.jmeter.protocol.java.sampler.JavaSamplerContext;
import org.apache.jmeter.samplers.Interruptible;
import org.apache.jmeter.samplers.SampleResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author jianyang
 * @date 2019-11-25
 */
public class SofaRpcSampler extends AbstractJavaSamplerClient implements Interruptible {

    private static final Logger LOG = LoggerFactory.getLogger(SofaRpcSampler.class);

    private final static ConsumerConfig<GenericService> consumerConfig = new ConsumerConfig<GenericService>();

    private final static String APPLICATON_NAME = "appName";

    private final static String INTERFACE_ID = "interfaceId";

    private final static String TIME_OUT = "timeout";

    private final static String DIRECT_URL = "directUrl";

    private final static String PROTOCL = "protocl";

    private String interfaceId = null;

    private static GenericService genericService = null;

    private final static Lock lock = new ReentrantLock();

    @Override
    public void setupTest(JavaSamplerContext context) {
        try {
            lock.lock();

            if (null != genericService ){
                return;
            }

            String rpcConfig = context.getParameter("rpcConfig");
            JsonObject rpcConfigJson = new JsonParser().parse(rpcConfig).getAsJsonObject();

            interfaceId = rpcConfigJson.get(INTERFACE_ID).getAsString();
            String directUrl = rpcConfigJson.get(DIRECT_URL).getAsString();
            Integer timeout =  rpcConfigJson.get(TIME_OUT).getAsInt();
            JsonElement protoclElement = rpcConfigJson.get(PROTOCL);
            String protocol = null == protoclElement ? "bolt" : protoclElement.getAsString();

            ApplicationConfig application = new ApplicationConfig().setAppName("jmeter-client");
            consumerConfig
                    .setApplication(application)
                    .setInterfaceId(interfaceId)
                    .setDirectUrl(directUrl)
                    .setTimeout(timeout)
                    .setRegister(false)
                    .setGeneric(true)
                    .setProtocol(protocol)
            ;

            genericService = consumerConfig.refer();
        } catch (Exception e) {
            LOG.error("rpc调用，客户端初始化失败。", e);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void teardownTest(JavaSamplerContext context) {
        super.teardownTest(context);
    }

    @Override
    public Arguments getDefaultParameters() {
        String rpcConfig = "{" +
                " \t\"interfaceId\" : \"com.jewin.jmeter.plugin.rpc.server.test.HelloService\",\n" +
                " \t\"timeout\" : \"3000\",\n" +
                " \t\"directUrl\" : \"10.19.13.51:22000\",\n" +
                " \t\"protocol\" : \"bolt\"\n" +
                " }";

        String invokeParameter = "{" +
                " \t\"argsCount\" : \"2\",\n" +
                "\t\"callMethodName\" : \"sayHello\",\n" +
                "\t\"arg0\" : {\n" +
                "\t\t\"clazzType\" : \"java.lang.String\",\n" +
                "\t\t\"argValue\" : \"jewin\"\n" +
                "\t},\n" +
                "\t\"arg1\" : {\n" +
                "\t\t\"clazzType\" : \"int\",\n" +
                "\t\t\"argValue\" : 29\n" +
                "\t\t}\n" +
                " }\t";

        Arguments params = new Arguments();
        params.addArgument("rpcConfig", rpcConfig);
        params.addArgument("invokeParameter", invokeParameter);
        return params;
    }

    public SampleResult runTest(JavaSamplerContext javaSamplerContext) {

        SampleResult result = new SampleResult();
        result.setDataType("text");

        try {
            String invokeParameter = javaSamplerContext.getParameter("invokeParameter");
            JsonObject invokeParamter = new JsonParser().parse(invokeParameter).getAsJsonObject();
            int argsCount = invokeParamter.get("argsCount").getAsInt();
            String callMethodName = invokeParamter.get("callMethodName").getAsString();

            String [] argsType = new String[argsCount];
            Object [] argValue = new Object[argsCount];

            for (int i = 0; i < argsCount; i++) {
                JsonObject argObject = invokeParamter.get("arg" + i).getAsJsonObject();
                argsType[i] = argObject.get("clazzType").getAsString();
                argValue[i] = parseArgValue(argObject.get("argValue").getAsString(), argsType[i]);
            }

            result.sampleStart();
            Object returnObject =  genericService.$invoke(callMethodName, argsType, argValue);
            result.sampleEnd();

            result.setSentBytes(invokeParamter.toString().getBytes("UTF-8").length);
            result.setSuccessful(true);
            result.setResponseMessageOK();
            result.setResponseCodeOK();
            result.setResponseData(null == returnObject ? "" : returnObject.toString(), "UTF-8");
        } catch (Exception e) {
            result.sampleEnd();

            LOG.error("rpc调用失败", e);
            result.setResponseCode("error");
            result.setResponseMessage(e.getMessage());
            result.setSuccessful(false);
        }

        return result;
    }

    private static final Map<String, Class<?>> BASE_JAVA_TYPE_MAP = new HashMap<String, Class<?>>();

    static {
        BASE_JAVA_TYPE_MAP.put("int", Integer.class);
        BASE_JAVA_TYPE_MAP.put("double", Double.class);
        BASE_JAVA_TYPE_MAP.put("long", Long.class);
        BASE_JAVA_TYPE_MAP.put("float", Float.class);
        BASE_JAVA_TYPE_MAP.put("byte", Byte.class);
        BASE_JAVA_TYPE_MAP.put("boolean", Boolean.class);
        BASE_JAVA_TYPE_MAP.put("char", Character.class);
        BASE_JAVA_TYPE_MAP.put("short", Short.class);
    }

    private Object parseArgValue(String argValue, String clazzType) throws ClassNotFoundException {
        if (StringUtil.isEmpty(clazzType)) {
            return argValue;
        }

        Gson gson = new Gson();
        Class<?> clazz = null;
        if (BASE_JAVA_TYPE_MAP.containsKey(clazzType)) {
            clazz = BASE_JAVA_TYPE_MAP.get(clazzType);
        } else {
            clazz = Class.forName(clazzType);
        }

        return gson.fromJson(argValue, clazz);
    }

    public boolean interrupt() {
        Thread t = Thread.currentThread();
        if (t != null) {
            t.interrupt();
        }

        return t != null;
    }

    public static void main(String[] args) {

        SofaRpcSampler ss = new SofaRpcSampler();
        Arguments arg = ss.getDefaultParameters();
        JavaSamplerContext context = new JavaSamplerContext(arg);

        ss.setupTest(context);
        ss.runTest(context);
        ss.teardownTest(context);
    }
}
