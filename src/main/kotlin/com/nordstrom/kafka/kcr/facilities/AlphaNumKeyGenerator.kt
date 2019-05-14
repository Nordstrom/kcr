package com.nordstrom.kafka.kcr.facilities

class AlphaNumKeyGenerator {

    fun key(len: Int): String {
        val charPool: List<Char> = ('a'..'z') + ('A'..'Z') + ('0'..'9')
        val sfx = (1..len)
            .map { i -> kotlin.random.Random.nextInt(0, charPool.size) }
            .map(charPool::get)
            .joinToString("");

        return sfx;
    }

}