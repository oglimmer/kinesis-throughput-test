package de.oglimmer.kinesis;

import lombok.AllArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Comparator;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/v1/admin")
@AllArgsConstructor
public class AdminController {

    private RuntimeConfiguration runtimeConfiguration;
    private RuntimeStatistics runtimeStatistics;
    private ReqRespVerifier reqRespVerifier;
    private DataGenerator dataGenerator;

    @GetMapping("stats")
    public String getStats() {
        StringBuilder buff = new StringBuilder();
        buff.append("********Mode: " + runtimeConfiguration.getMode() + "*********");
        buff.append("\r\n");
        buff.append("Unanswered Reqs=" + reqRespVerifier.getRequestUUIDs().size());
        buff.append("\r\n");
        if(reqRespVerifier.getRequestUUIDs().size() > 0) {
            buff.append("Oldest unanswered req=" + reqRespVerifier.getRequestUUIDs().values()
                    .stream()
                    .min(Comparator.comparing(BusMessage::getCreationTime))
                    .get().getCreationTime());
            buff.append("\r\n");
        }
        runtimeStatistics.getStreamStatsMap().entrySet().stream().forEach(e -> {
            buff.append("********Stream: " + e.getKey().name() + "*********");
            buff.append("\r\n");
            buff.append("RecordsGenerated=" + e.getValue().getRecordsGenerated());
            buff.append("\r\n");
            buff.append("RecordsAnswered=" + e.getValue().getRecordsAnswered());
            buff.append("\r\n");
            buff.append("lastMinutesGenerated=" + e.getValue().getRecordsGeneratedPerMinute());
            buff.append("\r\n");
            buff.append("lastMinutesAnswered=" + e.getValue().getRecordsAnsweredPerMinute());
            buff.append("\r\n");
        });
        runtimeStatistics.getOriginStatsMap().entrySet().stream().forEach(e -> {
            buff.append("********Origin: " + e.getKey().name() + "*********");
            buff.append("\r\n");
            buff.append("roundTripTimeAvg=" + e.getValue().getRoundTripTimeLastMinuteAvg());
            buff.append("\r\n");
        });
        if (reqRespVerifier.getRequestUUIDs().size() > 0) {
            buff.append("Unanswered=" + reqRespVerifier.getRequestUUIDs().values()
                    .stream()
                    .map(e -> e.getUuid()).collect(Collectors.toList()));
            buff.append("\r\n");
        }
        return buff.toString();
    }

    @PostMapping("reset")
    public void resetReqs() {
        reqRespVerifier.getRequestUUIDs().clear();
    }

    @PostMapping("start")
    public void startDataGen() {
        dataGenerator.getData().get(Stream.INBOUND).start();
    }

    @PostMapping("stop")
    public void stopDataGen() {
        dataGenerator.getData().get(Stream.INBOUND).stop();
    }

    @PostMapping("set-rate")
    public void setDataGenRate(@RequestBody long rate) {
        dataGenerator.getData().get(Stream.INBOUND).setDataRate(rate);
    }

}
