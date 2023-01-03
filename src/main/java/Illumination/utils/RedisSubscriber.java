package Illumination.utils;

import redis.clients.jedis.JedisPubSub;

public class RedisSubscriber extends JedisPubSub {
    public RedisSubscriber() {
    }

    @Override
    public void onMessage(String channel, String message) {       //收到消息會調用
        System.out.println(String.format("receive redis published message, channel %s, message %s", channel, message));
        this.unsubscribe();
    }

    @Override
    public void onPMessage(String pattern, String channel, String message) {
        System.out.println(String.format("PSubscribe receive message, channel %s, message %s", channel, message));
    }

    @Override
    public void onPSubscribe(String pattern, int subscribedChannels) {
        System.out.println(String.format("pSubscribe redis channel success, channel pattern %s,subscribedChannels %d", pattern, subscribedChannels));
    }

    @Override
    public void onSubscribe(String channel, int subscribedChannels) {    //訂閱頻道會調用
        System.out.println(String.format("subscribe redis channel success, channel %s, subscribedChannels %d",
                channel, subscribedChannels));
    }

    @Override
    public void onUnsubscribe(String channel, int subscribedChannels) {   //取消訂閱會調用
        System.out.println(String.format("unsubscribe redis channel, channel %s, subscribedChannels %d",
                channel, subscribedChannels));

    }
}
