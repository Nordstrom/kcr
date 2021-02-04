package com.nordstrom.kafka.kcr

import com.nordstrom.kafka.kcr.commands.PlayCommand
import com.nordstrom.kafka.kcr.commands.RecordCommand
import com.nordstrom.kafka.kcr.facilities.AlphaNumKeyGenerator
import com.nordstrom.kafka.kcr.kafka.KafkaOptions
import picocli.CommandLine
import kotlin.system.exitProcess

@CommandLine.Command(
    name = "kcr",
    mixinStandardHelpOptions = true,
    subcommands = [
        PlayCommand::class,
        RecordCommand::class,
        CommandLine.HelpCommand::class,
    ],
    usageHelpWidth = 120,
    versionProvider = CliVersionProvider::class
)
class Kcr : Runnable {
    @CommandLine.Mixin
    lateinit var kafkaOptions: KafkaOptions

    override fun run() = CommandLine(Kcr()).usage(System.out)

    companion object {
        val id: String = AlphaNumKeyGenerator().key(8)

        @JvmStatic
        fun main(args: Array<String>) {
            val exitCode = CommandLine(Kcr()).execute(*args)
            exitProcess(exitCode)
        }
    }
}