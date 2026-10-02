package org.black_ixx.playerpoints.commands;

import dev.rosewood.rosegarden.command.framework.CommandContext;
import dev.rosewood.rosegarden.command.framework.CommandInfo;
import dev.rosewood.rosegarden.command.framework.annotation.RoseExecutable;
import java.util.Map;
import org.black_ixx.playerpoints.PlayerPoints;
import org.black_ixx.playerpoints.manager.DataManager;

/** Read-only diagnostics: no JDBC, UUID enumeration or credentials in command output. */
public final class SyncStatusCommand extends BasePointsCommand {
    public SyncStatusCommand(PlayerPoints plugin) { super(plugin); }

    @RoseExecutable public void execute(CommandContext context) {
        Map<String, Long> status = this.playerPoints.getManager(DataManager.class).getPgSyncStatus();
        if (status.isEmpty()) context.getSender().sendMessage("PlayerPoints PG sync is disabled.");
        else context.getSender().sendMessage("PlayerPoints PG sync " + status);
    }

    @Override protected CommandInfo createCommandInfo() {
        return CommandInfo.builder("syncstatus").permission("playerpoints.syncstatus").build();
    }
}
