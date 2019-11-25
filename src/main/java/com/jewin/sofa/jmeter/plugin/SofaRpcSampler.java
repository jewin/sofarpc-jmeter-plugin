package com.jewin.sofa.jmeter.plugin;

import com.alipay.sofa.common.utils.StringUtil;
import com.alipay.sofa.rpc.api.GenericService;
import com.alipay.sofa.rpc.config.ApplicationConfig;
import com.alipay.sofa.rpc.config.ConsumerConfig;
import com.google.gson.Gson;
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

    private String interfaceId = null;
    private JsonObject paramter = null;


//    {
//        "genericConfig" : {
//                "interfaceId" : "com.xxx.com",
//                "timeout" : "1000",
//                "directUrl" : "127.0.0.1:21220"
//        },
//        "invokeParamter" : {
//                "argsCount" : "2",
//                "callMethodName" : "callme",
//                "arg0" : {
//                  "clazzType" : "java.lang.Integer",
//                    "argValue" : {}
//                  },
//                "arg1" : {
//                    "clazzType" : "java.lang.Integer",
//                    "argValue" : {}
//              }
//    }
//    }

    @Override
    public void setupTest(JavaSamplerContext context) {
        String jsonArgs = context.getParameter("args");
        paramter = new JsonParser().parse(jsonArgs).getAsJsonObject();
        JsonObject genericConfig = paramter.get("genericConfig").getAsJsonObject();

        interfaceId = genericConfig.get(INTERFACE_ID).getAsString();
        String directUrl = genericConfig.get(DIRECT_URL).getAsString();
        Integer timeout =  genericConfig.get(TIME_OUT).getAsInt();

        ApplicationConfig application = new ApplicationConfig().setAppName("jmeter-client");
        consumerConfig
                .setApplication(application)
                .setInterfaceId(interfaceId)
                .setDirectUrl(directUrl)
                .setTimeout(timeout)
                .setRegister(false)
                .setGeneric(true)
                .setProtocol("bolt")
        ;
    }

    @Override
    public void teardownTest(JavaSamplerContext context) {
        super.teardownTest(context);
        consumerConfig.unRefer();
    }

    @Override
    public Arguments getDefaultParameters() {
        String jsonArgs = "{\n" +
                " \"genericConfig\" : {\n" +
                " \t\"interfaceId\" : \"com.alipay.sofa.rpc.test.HelloService\",\n" +
                " \t\"timeout\" : \"300000\",\n" +
                " \t\"directUrl\" : \"bolt://127.0.0.1:22000?appName=xxx-server\"\n" +
                " },\n" +
                " \"invokeParameter\" : {\n" +
                " \t\"argsCount\" : \"2\",\n" +
                "\t\"callMethodName\" : \"sayHello\",\n" +
                "\t\"arg0\" : {\n" +
                "\t\t\"clazzType\" : \"java.lang.String\",\n" +
                "\t\t\"argValue\" : \"xx\"\n" +
                "\t},\n" +
                "\t\"arg1\" : {\n" +
                "\t\t\"clazzType\" : \"int\",\n" +
                "\t\t\"argValue\" : 29\n" +
                "\t\t}\n" +
                " \t}\t\n" +
                "}";

        Arguments params = new Arguments();
        params.addArgument("args", jsonArgs);
        return params;
    }

    public SampleResult runTest(JavaSamplerContext javaSamplerContext) {
        SampleResult result = new SampleResult();
        result.setDataType("text");

        try {
            JsonObject invokeParamter = paramter.get("invokeParameter").getAsJsonObject();
            int argsCount = invokeParamter.get("argsCount").getAsInt();
            String callMethodName = invokeParamter.get("callMethodName").getAsString();

            String [] argsType = new String[argsCount];
            Object [] argValue = new Object[argsCount];

            for (int i = 0; i < argsCount; i++) {
                JsonObject argObject = invokeParamter.get("arg" + i).getAsJsonObject();
                argsType[i] = argObject.get("clazzType").getAsString();
                argValue[i] = parseArgValue(argObject.get("argValue").getAsString(), argsType[i]);
            }

            GenericService genericService = consumerConfig.refer();

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
        String jsonArgs = "{\n" +
                " \"genericConfig\" : {\n" +
                " \t\"interfaceId\" : \"com.alipay.sofa.rpc.test.HelloService\",\n" +
                " \t\"timeout\" : \"300000\",\n" +
                " \t\"directUrl\" : \"bolt://127.0.0.1:22000?appName=xxx-server\"\n" +
                " },\n" +
                " \"invokeParameter\" : {\n" +
                " \t\"argsCount\" : \"2\",\n" +
                "\t\"callMethodName\" : \"sayHello\",\n" +
                "\t\"arg0\" : {\n" +
                "\t\t\"clazzType\" : \"java.lang.String\",\n" +
                "\t\t\"argValue\" : \"xx\"\n" +
                "\t},\n" +
                "\t\"arg1\" : {\n" +
                "\t\t\"clazzType\" : \"int\",\n" +
                "\t\t\"argValue\" : 29\n" +
                "\t\t}\n" +
                " \t}\t\n" +
                "}";
        Arguments arg = new Arguments();
        arg.addArgument("args", jsonArgs);
        JavaSamplerContext context = new JavaSamplerContext(arg);

        SofaRpcSampler ss = new SofaRpcSampler();
        ss.setupTest(context);
        ss.runTest(context);
        ss.teardownTest(context);
    }
}
