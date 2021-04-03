package com.ubbcluj.amcds.myDalgs.algorithms;

import com.ubbcluj.amcds.myDalgs.Process;
import com.ubbcluj.amcds.myDalgs.communication.Protocol;
import com.ubbcluj.amcds.myDalgs.globals.AbstractionType;
import com.ubbcluj.amcds.myDalgs.util.AbstractionIdUtil;

public class Application extends Abstraction {

    public Application(String abstractionId, Process process) {
        super(abstractionId, process);
        process.registerAbstraction(new BestEffortBroadcast(AbstractionIdUtil.getChildAbstractionId(abstractionId, AbstractionType.BEB), process));
        process.registerAbstraction(new PerfectLink(AbstractionIdUtil.getChildAbstractionId(abstractionId, AbstractionType.PL), process));

    }

    @Override
    public boolean handle(Protocol.Message message) {
        return false;
    }
}
