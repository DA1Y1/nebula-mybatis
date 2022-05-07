package com.mininglamp.nebulamybatis;

import com.google.common.collect.Lists;
import com.vesoft.nebula.Row;
import com.vesoft.nebula.Value;
import com.vesoft.nebula.client.graph.data.Node;
import com.vesoft.nebula.client.graph.data.PathWrapper;
import com.vesoft.nebula.client.graph.data.Relationship;
import com.vesoft.nebula.client.graph.data.ValueWrapper;
import com.vesoft.nebula.jdbc.NebulaStatement;
import lombok.extern.slf4j.Slf4j;
import org.apache.ibatis.executor.resultset.DefaultResultSetHandler;
import org.apache.ibatis.executor.resultset.ResultSetHandler;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.mapping.ResultMap;
import org.apache.ibatis.mapping.ResultMapping;
import org.apache.ibatis.plugin.*;
import org.springframework.beans.BeanUtils;
import org.springframework.jdbc.support.JdbcUtils;

import java.beans.PropertyDescriptor;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.*;
import java.util.stream.Collectors;

/**
 * mybatis interceptor扩展点
 * <p>
 * 扩展 ResultSetHandler接口 的 handleResultSets 方法
 * 在mybatis调用该方法时，转换resultset ，解析 nebula 的 ValueWrapper ，返回正确的对象
 * <p>
 * MyBatis 允许你在映射语句执行过程中的某一点进行拦截调用。默认情况下，MyBatis 允许使用插件来拦截的方法调用包括：
 * Executor (update, query, flushStatements, commit, rollback, getTransaction, close, isClosed)
 * ParameterHandler (getParameterObject, setParameters)
 * ResultSetHandler (handleResultSets, handleOutputParameters)
 * StatementHandler (prepare, parameterize, batch, update, query)
 * 通过 MyBatis 提供的强大机制，使用插件是非常简单的，只需实现 Interceptor 接口，并指定想要拦截的方法签名即可。
 *
 * @author Zhang Zhenhua
 */
@Slf4j
@Intercepts(@Signature(type = ResultSetHandler.class, method = "handleResultSets", args = Statement.class))
public class GraphResultSetIntercept implements Interceptor {

    @Override
    public Object intercept(Invocation invocation) throws Throwable {
        long start = System.currentTimeMillis();
        DefaultResultSetHandler target = (DefaultResultSetHandler) invocation.getTarget();
        Field field = target.getClass().getDeclaredField("mappedStatement");
        field.setAccessible(true);
        MappedStatement mappedStatement = (MappedStatement) field.get(target);
        List<ResultMap> resultMaps = mappedStatement.getResultMaps();
        boolean isValueWrapper = false;
        if (resultMaps != null && !resultMaps.isEmpty()) {
            if (resultMaps.size() > 1) {
                // 仅支持返回一种对象类型
                resultMaps.forEach(r -> log.error(r.getId()));
                throw new RuntimeException("不支持配置多项ResultMap或ResultType！");
            }
            for (ResultMap resultMap : resultMaps) {
                // 返回对象类
                Class<?> aClass = resultMap.getType();
                List<ResultMapping> resultMappings = resultMap.getResultMappings();
                if (!resultMappings.isEmpty()) {
                    // 返回对象的字段映射：数据库字段->bean字段
                    Map<String, String> propertiesMap = resultMappings.stream().collect(Collectors.toMap(ResultMapping::getColumn, ResultMapping::getProperty, (oldValue, newValue) -> oldValue, LinkedHashMap::new));
                    Statement statement = (Statement) invocation.getArgs()[0];
                    // sql返回值
                    ResultSet resultSet = statement.getResultSet();
                    if (!resultSet.next()) {
                        return invocation.proceed();
                    }
                    ResultSetMetaData metaData = resultSet.getMetaData();
                    // 每条数据的列数
                    int columnCount = metaData.getColumnCount();
                    List<Object> lists = new ArrayList<>();
                    List<com.vesoft.nebula.client.graph.data.ResultSet.Record> recordList = new ArrayList<>();
                    do {
                        // 生成一个对象，用于塞入本条数据
                        Object instantiateClass = BeanUtils.instantiateClass(aClass);
                        for (int index = 1; index <= columnCount; index++) {
                            // 当前列名
                            String column = JdbcUtils.lookupColumnName(metaData, index);
                            // 当前列的值
                            Object val = JdbcUtils.getResultSetValue(resultSet, index);
                            if (val == null) {
                                continue;
                            }
                            //nebula的返回值都是ValueWrapper，根据返回类使用不同方式解析
                            if (ValueWrapper.class.isAssignableFrom(val.getClass())) {
                                ValueWrapper wrapper = (ValueWrapper) val;
                                List<Value> values = new ArrayList<>();
                                Row row = new Row(values);
                                String decodeType;
                                int timezoneOffset;
                                if (wrapper.isVertex()) {
                                    isValueWrapper = true;
                                    Node node = wrapper.asNode();
                                    List<String> tags = node.tagNames();
                                    if (tags.size() > 1) {
                                        throw new RuntimeException("返回结果的包含多中tag，暂不支持");
                                    }
                                    values.add(node.getId().getValue());
                                    HashMap<String, ValueWrapper> properties = node.properties(tags.get(0));
                                    for (String v : propertiesMap.keySet()) {
                                        ValueWrapper fieldValueWrapper = properties.get(v);
                                        if ("id".equals(v)) {
                                            continue;
                                        }
                                        values.add(fieldValueWrapper == null ? null : fieldValueWrapper.getValue());
                                    }
                                    decodeType = node.getDecodeType();
                                    timezoneOffset = node.getTimezoneOffset();
                                    com.vesoft.nebula.client.graph.data.ResultSet.Record record = new com.vesoft.nebula.client.graph.data.ResultSet.Record(propertiesMap.keySet().stream().collect(Collectors.toList()), row, decodeType, timezoneOffset);
                                    recordList.add(record);
                                } else if (wrapper.isEdge()) {
                                    isValueWrapper = true;
                                    Relationship relationship = wrapper.asRelationship();
                                    values.add(relationship.srcId().getValue());
                                    values.add(relationship.dstId().getValue());
                                    HashMap<String, ValueWrapper> properties = relationship.properties();
                                    for (String v : propertiesMap.keySet()) {
                                        ValueWrapper fieldValueWrapper = properties.get(v);
                                        if ("_dst".equals(v) || "_src".equals(v)) {
                                            continue;
                                        }
                                        values.add(fieldValueWrapper == null ? null : fieldValueWrapper.getValue());
                                    }
                                    decodeType = relationship.getDecodeType();
                                    timezoneOffset = relationship.getTimezoneOffset();
                                    com.vesoft.nebula.client.graph.data.ResultSet.Record record = new com.vesoft.nebula.client.graph.data.ResultSet.Record(Lists.newArrayList(propertiesMap.keySet()), row, decodeType, timezoneOffset);
                                    recordList.add(record);
                                } else if (wrapper.isPath()) {
                                    doPath(aClass, propertiesMap, instantiateClass, wrapper);
                                } else {
                                    resultSet.beforeFirst();
                                    return invocation.proceed();
                                }
                            } else {
                                resultSet.beforeFirst();
                                return invocation.proceed();
                            }
                        }
                        lists.add(instantiateClass);
                    } while (resultSet.next());
                    log.debug("interceptor 消耗时间 ： " + (System.currentTimeMillis() - start));
                    if (isValueWrapper) {
                        com.mininglamp.nebulamybatis.NebulaResultSet graphResultSet = new NebulaResultSet(recordList.size(), Lists.newArrayList(propertiesMap.keySet()), recordList);
                        NebulaStatement graphStatement = new NebulaStatement(graphResultSet);
                        Invocation newInvocation = new Invocation(target, invocation.getMethod(), new Object[]{graphStatement});
                        return newInvocation.proceed();
                    }
                    return lists;
                }
            }
        }
        return invocation.proceed();
    }

    /**
     * 处理 PATH 类 ValueWrapper
     * <p>
     * Path比较特殊，提供了一种通用的对象接收【StringPathDO、LongPathDO】，也必须使用我们提供的path对象才能正确返回PathWrapper ，否则不能使用find path等语句
     *
     * @param aClass           返回对象类型
     * @param propertiesMap    字段映射
     * @param instantiateClass 返回对象
     * @param wrapper          数据
     * @throws UnsupportedEncodingException
     * @throws NoSuchFieldException
     * @throws IllegalAccessException
     * @throws InvocationTargetException
     */
    private void doPath(Class<?> aClass, Map<String, String> propertiesMap, Object instantiateClass, ValueWrapper wrapper) throws UnsupportedEncodingException, NoSuchFieldException, IllegalAccessException, InvocationTargetException {
        // 转换 PathWrapper
        PathWrapper path = wrapper.asPath();
        //注入起点终点字段
        writeField(aClass, propertiesMap, instantiateClass, "_start", path.getStartNode().getId());
        writeField(aClass, propertiesMap, instantiateClass, "_end", path.getEndNode().getId());
        //链路上所有的 节点id
        List<Object> collect = path.getNodes().stream().map(Node::getId).map(valueWrapper -> {
            try {
                return valueWrapper.isString() ? valueWrapper.asString() : valueWrapper.asLong();
            } catch (UnsupportedEncodingException e) {
                log.error("参数转换异常");
                e.printStackTrace();
            }
            return null;
        }).collect(Collectors.toList());
        //注入nodes
        if (propertiesMap.containsKey("_nodes")) {
            Field subField = aClass.getDeclaredField(propertiesMap.get("_nodes"));
            PropertyDescriptor pd = BeanUtils.getPropertyDescriptor(aClass, subField.getName());
            if (pd != null) {
                Method writeMethod = pd.getWriteMethod();
                writeMethod.invoke(instantiateClass, collect);
            }
        }
        //注入RelationShip
        if (propertiesMap.containsKey("_relation_ships")) {
            List<Object> relationShips = new ArrayList<>();
            for (Relationship relationship : path.getRelationships()) {
                //获取约定的内部类$RelationShip
                Class<?> innerClass;
                try {
                    innerClass = Class.forName(aClass.getName() + "$RelationShip");
                } catch (ClassNotFoundException e) {
                    throw new RuntimeException("resultMap中的type类型请使用提供的StringPathDO、LongPathDO");
                }
                PropertyDescriptor src = BeanUtils.getPropertyDescriptor(innerClass, "src");
                PropertyDescriptor dst = BeanUtils.getPropertyDescriptor(innerClass, "dst");
                PropertyDescriptor ranking = BeanUtils.getPropertyDescriptor(innerClass, "ranking");
                if (src == null || dst == null || ranking == null) {
                    throw new RuntimeException("resultMap中的type类型请使用提供的StringPathDO、LongPathDO");
                }
                Method srcWriteMethod = src.getWriteMethod();
                Method dstWriteMethod = dst.getWriteMethod();
                Method rankingWriteMethod = ranking.getWriteMethod();
                if (srcWriteMethod == null || dstWriteMethod == null || rankingWriteMethod == null) {
                    throw new RuntimeException("resultMap中的type类型请使用提供的StringPathDO、LongPathDO");
                }
                //依次注入所有的RelationShip
                Object inner = BeanUtils.instantiateClass(innerClass);
                if (relationship.srcId().isString()) {
                    srcWriteMethod.invoke(inner, relationship.srcId().asString());
                    dstWriteMethod.invoke(inner, relationship.dstId().asString());
                } else {
                    srcWriteMethod.invoke(inner, relationship.srcId().asLong());
                    dstWriteMethod.invoke(inner, relationship.dstId().asLong());
                }
                rankingWriteMethod.invoke(inner, relationship.ranking());
                relationShips.add(inner);
            }
            //将以上统计的关系，注入RelationShips
            Field subField = aClass.getDeclaredField(propertiesMap.get("_relation_ships"));
            PropertyDescriptor pd = BeanUtils.getPropertyDescriptor(aClass, subField.getName());
            if (pd != null) {
                Method writeMethod = pd.getWriteMethod();
                writeMethod.invoke(instantiateClass, relationShips);
            }
        }
    }

    /**
     * 处理 Relationship 类 ValueWrapper
     * <p>
     * 统一了边对象的定义，起点和终点在resultmap中的列名需要定义为_src，_dst ，rank需要定义为 _rank
     *
     * @param aClass           返回对象类型
     * @param propertiesMap    字段映射
     * @param instantiateClass 返回对象
     * @param wrapper          数据
     * @throws NoSuchFieldException
     * @throws UnsupportedEncodingException
     * @throws IllegalAccessException
     * @throws InvocationTargetException
     */
    @Deprecated
    private void doEdge(Class<?> aClass, Map<String, String> propertiesMap, Object instantiateClass, ValueWrapper wrapper) throws NoSuchFieldException, UnsupportedEncodingException, IllegalAccessException, InvocationTargetException {
        Relationship relationship = wrapper.asRelationship();
        writeField(aClass, propertiesMap, instantiateClass, "_src", relationship.srcId());
        writeField(aClass, propertiesMap, instantiateClass, "_dst", relationship.dstId());
        if (propertiesMap.containsKey("_rank")) {
            Field subField = aClass.getDeclaredField(propertiesMap.get("_rank"));
            PropertyDescriptor pd = BeanUtils.getPropertyDescriptor(aClass, subField.getName());
            if (pd != null) {
                Method writeMethod = pd.getWriteMethod();
                writeMethod.invoke(instantiateClass, relationship.ranking());
            }
        }
        HashMap<String, ValueWrapper> properties = relationship.properties();
        for (String key : relationship.keys()) {
            ValueWrapper fieldValueWrapper = properties.get(key);
            writeField(aClass, propertiesMap, instantiateClass, key, fieldValueWrapper);
        }
    }

    /**
     * 处理 Node 类 ValueWrapper
     * <p>
     * 需要resultmap中包含列名为id的列，否则无法注入id
     *
     * @param aClass           返回对象类型
     * @param propertiesMap    字段映射
     * @param instantiateClass 返回对象
     * @param wrapper          数据
     * @throws UnsupportedEncodingException
     * @throws NoSuchFieldException
     * @throws IllegalAccessException
     * @throws InvocationTargetException
     */
    @Deprecated
    private void doVertex(Class<?> aClass, Map<String, String> propertiesMap, Object instantiateClass, ValueWrapper wrapper) throws UnsupportedEncodingException, NoSuchFieldException, IllegalAccessException, InvocationTargetException {
        Node node = wrapper.asNode();
        List<String> tags = node.tagNames();
        if (tags.size() > 1) {
            throw new RuntimeException("返回结果的包含多中tag，暂不支持");
        }
        writeField(aClass, propertiesMap, instantiateClass, "id", node.getId());
        HashMap<String, ValueWrapper> properties = node.properties(tags.get(0));
        for (String key : node.keys(tags.get(0))) {
            ValueWrapper fieldValueWrapper = properties.get(key);
            writeField(aClass, propertiesMap, instantiateClass, key, fieldValueWrapper);
        }
    }

    /**
     * 注入字段
     *
     * @param aClass           返回对象类型
     * @param propertiesMap    字段映射
     * @param instantiateClass 返回对象
     * @param column           列名
     * @param wrapper          数据
     * @throws NoSuchFieldException
     * @throws UnsupportedEncodingException
     * @throws IllegalAccessException
     * @throws InvocationTargetException
     */
    private void writeField(Class<?> aClass, Map<String, String> propertiesMap, Object instantiateClass, String column, ValueWrapper wrapper) throws NoSuchFieldException, UnsupportedEncodingException, IllegalAccessException, InvocationTargetException {
        if (!wrapper.isNull() && propertiesMap.containsKey(column)) {
            Field subField = aClass.getDeclaredField(propertiesMap.get(column));
            Object o = resolveValue(wrapper, subField);
            PropertyDescriptor pd = BeanUtils.getPropertyDescriptor(aClass, subField.getName());
            if (pd != null) {
                Method writeMethod = pd.getWriteMethod();
                writeMethod.invoke(instantiateClass, o);
            }
        }
    }

    /**
     * 解析基本数据类型ValueWrapper
     *
     * @param valueWrapper 数据
     * @param field        字段
     * @return
     * @throws UnsupportedEncodingException
     */
    private Object resolveValue(ValueWrapper valueWrapper, Field field) throws UnsupportedEncodingException {
        if (valueWrapper.isBoolean()) {
            return valueWrapper.asBoolean();
        } else if (valueWrapper.isLong()) {
            long l = valueWrapper.asLong();
            if (field.getType().isAssignableFrom(Timestamp.class)) {
                return (new Timestamp(l * 1000));
            } else {
                return l;
            }
        } else if (valueWrapper.isDate()) {
            return valueWrapper.asDate();
        } else if (valueWrapper.isDateTime()) {
            return valueWrapper.asDateTime();
        } else if (valueWrapper.isTime()) {
            return valueWrapper.asTime();
        } else if (valueWrapper.isDouble()) {
            return valueWrapper.asDouble();
        } else if (valueWrapper.isList()) {
            ArrayList<ValueWrapper> valueWrappers = valueWrapper.asList();
            List<Object> temp = new ArrayList<>();
            for (ValueWrapper wrapper : valueWrappers) {
                Object o = resolveValue(wrapper, field);
                temp.add(o);
            }
            return temp;
        } else if (valueWrapper.isMap()) {
            HashMap<String, ValueWrapper> stringValueWrapperHashMap = valueWrapper.asMap();
            HashMap<String, Object> tmp = new HashMap<>(stringValueWrapperHashMap.size());
            for (Map.Entry<String, ValueWrapper> stringValueWrapperEntry : stringValueWrapperHashMap.entrySet()) {
                tmp.put(stringValueWrapperEntry.getKey(), resolveValue(stringValueWrapperEntry.getValue(), field));
            }
            return tmp;
        } else if (valueWrapper.isNull()) {
            return null;
        } else if (valueWrapper.isSet()) {
            HashSet<ValueWrapper> valueWrappers = valueWrapper.asSet();
            HashSet<Object> tmp = new HashSet<>();
            for (ValueWrapper wrapper : valueWrappers) {
                tmp.add(resolveValue(wrapper, field));
            }
            return tmp;
        } else if (valueWrapper.isString()) {
            return valueWrapper.asString();
        } else {
            throw new RuntimeException("未识别的valueWrapper");
        }
    }

    @Override
    public Object plugin(Object target) {
        return Plugin.wrap(target, this);
    }

    @Override
    public void setProperties(Properties properties) {
    }
}
