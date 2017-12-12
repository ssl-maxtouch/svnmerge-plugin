package jenkins.plugins.svnmerge;

import hudson.Extension;
import hudson.FilePath.FileCallable;
import hudson.model.AbstractBuild;
import hudson.model.AbstractProject;
import hudson.model.Action;
import hudson.model.BuildListener;
import hudson.model.Item;
import hudson.model.JobProperty;
import hudson.model.JobPropertyDescriptor;
import hudson.model.TaskListener;
import hudson.model.listeners.ItemListener;
import hudson.remoting.VirtualChannel;
import hudson.scm.SCM;
import hudson.scm.SubversionEventHandlerImpl;
import hudson.scm.SubversionSCM;
import hudson.scm.SvnClientManager;
import hudson.scm.SubversionSCM.ModuleLocation;
import hudson.util.IOException2;
import jenkins.model.Jenkins;
import net.sf.json.JSONObject;

import org.kohsuke.stapler.DataBoundConstructor;
import org.kohsuke.stapler.StaplerRequest;
import org.tmatesoft.svn.core.ISVNLogEntryHandler;
import org.tmatesoft.svn.core.SVNCommitInfo;
import org.tmatesoft.svn.core.SVNException;
import org.tmatesoft.svn.core.SVNLogEntry;
import org.tmatesoft.svn.core.SVNURL;
import org.tmatesoft.svn.core.auth.ISVNAuthenticationProvider;
import org.tmatesoft.svn.core.wc.ISVNEventHandler;
import org.tmatesoft.svn.core.wc.SVNClientManager;
import org.tmatesoft.svn.core.wc.SVNCommitClient;
import org.tmatesoft.svn.core.wc.SVNEvent;
import org.tmatesoft.svn.core.wc.SVNInfo;
import org.tmatesoft.svn.core.wc.SVNRevision;
import org.tmatesoft.svn.core.wc.SVNStatusType;
import org.tmatesoft.svn.core.wc.SVNUpdateClient;
import org.tmatesoft.svn.core.wc.SVNWCClient;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.lang.mutable.MutableBoolean;

import static org.tmatesoft.svn.core.SVNDepth.*;
import org.tmatesoft.svn.core.SVNPropertyValue;
import org.tmatesoft.svn.core.wc.ISVNStatusHandler;
import org.tmatesoft.svn.core.wc.SVNConflictChoice;
import org.tmatesoft.svn.core.wc.SVNDiffClient;
import org.tmatesoft.svn.core.wc.SVNPropertyData;
import static org.tmatesoft.svn.core.wc.SVNRevision.*;
import org.tmatesoft.svn.core.wc.SVNRevisionRange;
import org.tmatesoft.svn.core.wc.SVNStatus;

/**
 * {@link JobProperty} for feature branch projects.
 * <p>
 * This associates the upstream project (with {@link IntegratableProject} with this project.
 *
 * @author Kohsuke Kawaguchi
 */
public class FeatureBranchProperty extends JobProperty<AbstractProject<?,?>> implements Serializable
{
    private static final long serialVersionUID = -1L;

    /**
     * Upstream job name.
     */
    private String upstream;
    private transient RebaseAction rebaseAction;

    @DataBoundConstructor
    public FeatureBranchProperty(String upstream) {
        if (upstream == null) {
            throw new NullPointerException("upstream");
        }
        this.upstream = upstream;
    }

    public String getUpstream() {
        return upstream;
    }

    /**
     * Gets the upstream project, or null if no such project was found.
     */
    public AbstractProject<?,?> getUpstreamProject() {
        return Jenkins.getInstance().getItemByFullName(upstream,AbstractProject.class);
    }

    public ModuleLocation getUpstreamSubversionLocation() {
        AbstractProject<?,?> p = getUpstreamProject();
        if(p==null)     return null;
        SCM scm = p.getScm();
        if (scm instanceof SubversionSCM) {
            SubversionSCM svn = (SubversionSCM) scm;
            ModuleLocation ml = svn.getLocations()[0];
            // expand system and node environment variables as well as the project parameters
            ml = Utility.getExpandedLocation(ml, p);
            return ml;
        }
        return null;
    }

    /**
     * Gets the {@link #getUpstreamSubversionLocation()} as {@link SVNURL}
     */
    public SVNURL getUpstreamURL() throws SVNException {
        ModuleLocation location = getUpstreamSubversionLocation();
        if(location==null)  return null;
        return location.getSVNURL();
    }

    public AbstractProject<?,?> getOwner() {
        return owner;
    }

    @Override
    public List<Action> getJobActions(AbstractProject<?,?> project) {
        if (rebaseAction==null)
            rebaseAction = new RebaseAction(project);
        return Arrays.asList(new IntegrationStatusAction(this), rebaseAction);
    }

    /**
     * Just add the integration action.
     */
    @Override
    public boolean prebuild(AbstractBuild<?, ?> build, BuildListener listener) {
        build.addAction(new IntegrateAction(build));
        return true;
    }

    /**
     * Integrates changes made in the upstream into the branch at the workspace.
     *
     * <p>
     * This computation uses the workspace of the project. First, we update the workspace
     * to the tip of the branch (or else the commit will fail later), merge the changes
     * from the upstream, then commit it. If the merge fails, we'll revert the workspace
     * so that the next build can go smoothly.
     *
     * @param listener
     *      Where the progress is sent.
     * @param upstreamRev
     *      Revision of the upstream to rebase from.
     *      If -1, use the latest.
     * @return
     *      the new revision number if the rebase was successful.
     *      -1 if it failed and the failure was handled gracefully
     *      (typically this means a merge conflict.)
     */
    public long rebase(final TaskListener listener, final long upstreamRev) throws IOException, InterruptedException
    {
        final SubversionSCM svn = (SubversionSCM) getOwner().getScm();
        final ISVNAuthenticationProvider provider = svn.createAuthenticationProvider(getOwner(), svn.getLocations()[0]);

        final ModuleLocation upstreamLocation = getUpstreamSubversionLocation();

        AbstractBuild build = owner.getSomeBuildWithWorkspace();
        if (build == null)
        {
            final PrintStream logger = listener.getLogger();
            logger.print("No workspace found for project! Please perform a build first.\n");
            return -1L;
        }
        return build.getModuleRoot().act(
            new FileCallable<Long>()
            {
                public Long invoke(File mr, VirtualChannel virtualChannel) throws IOException
                {
                    try
                    {
                        final PrintStream logger = listener.getLogger();
                        final boolean[] foundConflict = new boolean[1];
                        ISVNEventHandler printHandler = new SubversionEventHandlerImpl(logger, mr)
                        {
                            @Override
                            public void handleEvent(SVNEvent event, double progress) throws SVNException
                            {
                                super.handleEvent(event, progress);
                                if (event.getContentsStatus() == SVNStatusType.CONFLICTED
                                    || event.getContentsStatus() == SVNStatusType.CONFLICTED_UNRESOLVED)
                                {
                                    foundConflict[0] = true;
                                }
                            }
                        };

                        SvnClientManager svnm = SubversionSCM.createClientManager(provider);

                        SVNURL up = upstreamLocation.getSVNURL();
                        SVNURL job_svn_url = svn.getLocations()[0].getSVNURL();

                        SVNClientManager cm = svnm.getCore();
                        cm.setEventHandler(printHandler);

                        SVNWCClient wc = cm.getWCClient();

                        final long[] create_n_last_rebase = parse_branch_log(job_svn_url, cm, logger);
                        /*{localCreate, upstreamCreate, localRebase, upstreamRebase}*/
                        final long create_or_last_rebase = create_n_last_rebase[2] > 0 ? create_n_last_rebase[2] : create_n_last_rebase[0];

                        SVNRevision mergeRev = upstreamRev >= 0 ? SVNRevision.create(upstreamRev) : wc.doInfo(up, HEAD, HEAD).getCommittedRevision();
                        if (mergeRev.getNumber() <= create_or_last_rebase)
                        {
                            logger.println("  No new commits since the last rebase. This rebase was a no-op.");
                            logger_print_build_status(logger, true);
                            return 0L;
                        }

                        execute_workspace_svn_prepare(mr, job_svn_url, cm, logger);

                        execute_merge(mr,
                                      up,
                                      create_or_last_rebase,
                                      mergeRev,
                                      cm,
                                      logger);

                        if(foundConflict[0])
                        {
                            logger_print_merge_conflict(logger, wc.doInfo(mr, null).getURL().toString(), up.toString());
                            logger_print_build_status(logger, false);
                            return -1L;
                        }
                        else
                        {
                            try
                            {
                                final String commit_msg = RebaseAction.COMMIT_MESSAGE_PREFIX + "Rebasing from " + up + "@" + mergeRev;
                                SVNCommitInfo ci = execute_commit(mr, commit_msg, cm, logger);
                                if (ci.getNewRevision() < 0)
                                {
                                    logger.println("  No changes since the last rebase. This rebase was a no-op.");
                                    logger_print_build_status(logger, true);
                                    return 0L;
                                }
                                else
                                {
                                    logger.println("  committed revision " + ci.getNewRevision());
                                    logger_print_build_status(logger, true);
                                    return ci.getNewRevision();
                                }
                            }
                            catch (SVNException e)
                            {
                                logger.println(e.getLocalizedMessage());
                                logger_print_merge_conflict(logger, wc.doInfo(mr, null).getURL().toString(), up.toString());
                                logger_print_build_status(logger, false);
                                return -1L;
                            }
                        }
                    }
                    catch (SVNException e)
                    {
                        throw new IOException2("Failed to merge", e);
                    }
                }
            }
        );
    }

    /**
     * Represents the result of integration.
     */
    public static class IntegrationResult implements Serializable
    {
        private static final long serialVersionUID = -1L;

        /**
         * The merge commit in the upstream where the integration is made visible to the upstream.
         * Or 0 if the integration was no-op and no commit was made.
         * -1 if it failed and the failure was handled gracefully
         * (typically this means a merge conflict.)
         */
        public final long mergeCommit;

        /**
         * The commit in the branch that was merged (or attempted to be merged.)
         */
        public final long integrationSource;

        public IntegrationResult(long mergeCommit, SVNRevision integrationSource)
        {
            this.mergeCommit = mergeCommit;
            this.integrationSource = integrationSource.getNumber();
            assert this.integrationSource!=-1L;
        }
    }

    /**
     * Perform a merge to the upstream that integrates changes in this branch.
     *
     * <p>
     * This computation uses the workspace of the project.
     *
     * @param listener
     *      Where the progress is sent.
     * @param branchURL
     *      URL of the branch to be integrated. If null, use the workspace URL.
     * @param branchRev
     *      Revision of the branch to be integrated to the upstream.
     *      If -1, use the current workspace revision.
     * @param commitMessage
     * @return
     *      Always non-null. See {@link IntegrationResult}
     * @throws java.io.IOException
     * @throws java.lang.InterruptedException
     */
    public IntegrationResult integrate(final TaskListener listener, final String branchURL, final long branchRev, final String commitMessage) throws IOException, InterruptedException
    {
        final Long lastIntegrationSourceRevision = getlastIntegrationSourceRevision();

        final SubversionSCM svn = (SubversionSCM) getUpstreamProject().getScm();
        final ISVNAuthenticationProvider provider = svn.createAuthenticationProvider(getUpstreamProject(), svn.getLocations()[0]);

        final ModuleLocation upstreamLocation = getUpstreamSubversionLocation();

        return owner.getModuleRoot().act(
            new FileCallable<IntegrationResult>()
            {
                public IntegrationResult invoke(File mr, VirtualChannel virtualChannel) throws IOException
                {
                    try
                    {
                        final PrintStream logger = listener.getLogger();
                        final boolean[] foundConflict = new boolean[1];
                        ISVNEventHandler printHandler = new SubversionEventHandlerImpl(logger, mr)
                        {
                            @Override
                            public void handleEvent(SVNEvent event, double progress) throws SVNException
                            {
                                super.handleEvent(event, progress);
                                if (event.getContentsStatus() == SVNStatusType.CONFLICTED
                                    || event.getContentsStatus() == SVNStatusType.CONFLICTED_UNRESOLVED)
                                {
                                    foundConflict[0] = true;
                                }
                            }
                        };

                        SvnClientManager svnm = SubversionSCM.createClientManager(provider);

                        SVNURL up = upstreamLocation.getSVNURL();
                        SVNURL job_svn_url = svn.getLocations()[0].getSVNURL();

                        SVNClientManager cm = svnm.getCore();
                        cm.setEventHandler(printHandler);

                        SVNWCClient wc = cm.getWCClient();
                        SVNURL mergeUrl = branchURL != null ? SVNURL.parseURIDecoded(branchURL) : job_svn_url;
                        SVNRevision mergeRev = branchRev >= 0 ? SVNRevision.create(branchRev) : wc.doInfo(mergeUrl, HEAD, HEAD).getCommittedRevision();

                        // do we have any meaningful changes in this branch worthy of integration?
                        if (lastIntegrationSourceRevision != null)
                        {
                            final boolean changesFound = integrate_check_necessary(mergeUrl,
                                                                                   mergeRev,
                                                                                   lastIntegrationSourceRevision,
                                                                                   cm,
                                                                                   logger);
                            if (!changesFound)
                            {
                                logger.println("No changes to be integrated. Skipping integration.");
                                return new IntegrationResult(0, mergeRev);
                            }
                        }

                        final long[] create_n_last_rebase = parse_branch_log(mergeUrl, cm, logger);
                        /*{localCreate, upstreamCreate, localRebase, upstreamRebase}*/
                        final long create_or_last_rebase = create_n_last_rebase[2] > 0 ? create_n_last_rebase[2] : create_n_last_rebase[0];

                        logger.println("The first revision of this branch is " + create_n_last_rebase[0]);

                        execute_workspace_svn_prepare(mr, up, cm, logger);

                        final long integrate_from = lastIntegrationSourceRevision != null ? lastIntegrationSourceRevision : create_or_last_rebase;
                        execute_merge(mr,
                                      mergeUrl,
                                      integrate_from,
                                      mergeRev,
                                      cm,
                                      logger);

                        if(foundConflict[0])
                        {
                            logger_print_merge_conflict(logger, wc.doInfo(mr, null).getURL().toString(), mergeUrl.toString());
                            logger_print_build_status(logger, false);
                            return new IntegrationResult(-1L, mergeRev);
                        }

                        long trunkCommit;

                        String commit_msg = commitMessage + "\n" + mergeUrl + "@" + mergeRev;
                        SVNCommitInfo ci;
                        try
                        {
                            ci = execute_commit(mr, commit_msg, cm, logger);
                        }
                        catch (SVNException e)
                        {
                            logger.println(e.getLocalizedMessage());
                            logger_print_merge_conflict(logger, wc.doInfo(mr, null).getURL().toString(), mergeUrl.toString());
                            logger_print_build_status(logger, false);
                            return new IntegrationResult(-1L, mergeRev);
                        }
                        if(ci.getNewRevision() < 0)
                        {
                            trunkCommit = 0L;
                            logger.println("  No changes since the last integration");
                        }
                        else
                        {
                            trunkCommit = ci.getNewRevision();
                            logger.println("  committed revision " + trunkCommit);

                            logger.println("\nSwitching back to branch\n");
                            execute_workspace_svn_prepare(mr, mergeUrl, cm, logger);

                            execute_merge(mr,
                                          up,
                                          create_or_last_rebase,
                                          SVNRevision.create(trunkCommit),
                                          cm,
                                          logger);
                            wc.doResolve(mr, INFINITY, true, true, true, SVNConflictChoice.MERGED);

                            commit_msg = RebaseAction.COMMIT_MESSAGE_PREFIX + "Rebasing from our integrate to " + up + "@" + trunkCommit;
                            SVNCommitInfo bci = execute_commit(mr, commit_msg, cm, logger);

                            logger.println("  committed revision " + bci.getNewRevision());
                        }

                        logger_print_build_status(logger, true);

                        return new IntegrationResult(trunkCommit, mergeRev);
                    }
                    catch (SVNException e)
                    {
                        throw new IOException("Failed to merge", e);
                    }
                }
            }
        );
    }

    private Long getlastIntegrationSourceRevision()
    {
        IntegrateAction ia = IntegrationStatusAction.getLastIntegrateAction(owner);
        if (ia != null)   return ia.getIntegrationSource();
        return null;
    }

    /**
     * If an upstream is renamed, update the configuration accordingly.
     */
    @Extension
    public static class ItemListenerImpl extends ItemListener {
        @Override
        public void onRenamed(Item item, String oldName, String newName) {
            if (item instanceof AbstractProject) {
                AbstractProject<?,?> up = (AbstractProject) item;
                if(up.getProperty(IntegratableProject.class)!=null) {
                    try {
                        for (AbstractProject<?,?> p : Jenkins.getInstance().getItems(AbstractProject.class)) {
                            FeatureBranchProperty fbp = p.getProperty(FeatureBranchProperty.class);
                            if(fbp!=null) {
                                if(fbp.upstream.equals(oldName)) {
                                    fbp.upstream=newName;
                                    p.save();
                                }
                            }
                        }
                    } catch (IOException e) {
                        LOGGER.log(Level.WARNING, "Failed to persist configuration", e);
                    }
                }
            }
        }
    }

    @Extension
    public static final class DescriptorImpl extends JobPropertyDescriptor {
        @Override
        public JobProperty<?> newInstance(StaplerRequest req, JSONObject formData) throws FormException {
            if(!formData.has("svnmerge"))   return null;
            return req.bindJSON(FeatureBranchProperty.class, formData.getJSONObject("svnmerge"));
        }

        public String getDisplayName() {
            return "Upstream Subversion branch";
        }

        public List<AbstractProject<?,?>> listIntegratableProjects() {
            List<AbstractProject<?,?>> r = new ArrayList<AbstractProject<?,?>>();
            for(AbstractProject<?,?> p : Jenkins.getInstance().getItems(AbstractProject.class))
                if(p.getProperty(IntegratableProject.class)!=null)
                    r.add(p);
            return r;
        }
    }

    private static final Logger LOGGER = Logger.getLogger(FeatureBranchProperty.class.getName());

    private long[] parse_branch_log(final SVNURL branch_svn_url, final SVNClientManager cm, final PrintStream logger) throws SVNException
    {
        final long[] ret_array = {0, 0, 0, 0}; // {localCreate, upstreamCreate, localRebase, upstreamRebase}

        logger.println("Parsing log of " + branch_svn_url);

        // https://svnkit.com/javadoc/org/tmatesoft/svn/core/wc/SVNLogClient.html
        cm.getLogClient().doLog(
            branch_svn_url,
            null,     /*paths*/
            HEAD,     /*pegRevision*/
            HEAD,     /*startRevision*/
            SVNRevision.create(0), /*endRevision*/
            true,     /*stopOnCopy*/
            false,    /*discoverChangedPaths*/
            0,        /*limit*/
            new ISVNLogEntryHandler()
            {
                final Pattern pattern_rebase = Pattern.compile("\\[REBASE\\].+@(\\d+)");
                final Pattern pattern_create = Pattern.compile("\\[CREATE\\].+\\?r=(\\d+)");
                Matcher matcher;
                public void handleLogEntry(SVNLogEntry e) throws SVNException
                {
                    matcher = pattern_create.matcher(e.getMessage());
                    if (matcher.find())
                    {
                        ret_array[0] = e.getRevision();
                        ret_array[1] = Long.parseLong(matcher.group(1));
                        logger.println("Found the create at r" + ret_array[0] + " - upstream r" + ret_array[1]);
                    }
                    else if (0 == ret_array[2])
                    {
                        matcher = pattern_rebase.matcher(e.getMessage());
                        if (matcher.find())
                        {
                            ret_array[2] = e.getRevision();
                            ret_array[3] = Long.parseLong(matcher.group(1));
                            logger.println("Found a rebase at r" + ret_array[2] + " - upstream r" + ret_array[3]);
                        }
                    }
                }
            }
        );
        return ret_array;
    }

    private String get_svn_mergeinfo(final File mr, final SVNWCClient wc) throws SVNException
    {
        // https://wiki.svnkit.com/Managing_A_Working_Copy
        // https://svnkit.com/javadoc/org/tmatesoft/svn/core/wc/SVNWCClient.html
        SVNPropertyData mergeinfo_property = wc.doGetProperty(mr, /*path*/
                                                              "svn:mergeinfo", /*propName*/
                                                              HEAD, /*pegRevision*/
                                                              HEAD); /*revision*/
        final SVNPropertyValue mergeinfo_property_value = mergeinfo_property.getValue();
        final String mergeinfo_string = mergeinfo_property_value.getString();
        return mergeinfo_string;
    }

    private void set_svn_mergeinfo(final File mr, final SVNWCClient wc, final String svn_mergeinfo) throws SVNException
    {
        wc.doSetProperty(mr, /*path*/
                         "svn:mergeinfo", /*propName*/
                         SVNPropertyValue.create(svn_mergeinfo), /*propValue*/
                         false, /*skipChecks*/
                         EMPTY, /*depth */
                         null, /*handler */
                         null); /*changeLists */
    }

    private String get_path_rel_to_repo_root(final File mr, final SVNWCClient wc) throws SVNException
    {
        final SVNInfo svn_info = wc.doInfo(mr, null);
        return svn_info.getURL().toString().replace(svn_info.getRepositoryRootURL().toString(), "");
    }

    private String get_path_rel_to_repo_root(final SVNURL svn_url, final SVNWCClient wc) throws SVNException
    {
        final SVNInfo svn_info = wc.doInfo(svn_url, HEAD, HEAD);
        return svn_info.getURL().toString().replace(svn_info.getRepositoryRootURL().toString(), "");
    }

    private long get_latest_merged_rev_from_mergeinfo(final String svn_mergeinfo, final String merge_path_rel_to_repo_root)
    {
        final Pattern pattern_mergeinfo = Pattern.compile(merge_path_rel_to_repo_root+"(:\\d+-|:)(\\d+)");
        Matcher matcher = pattern_mergeinfo.matcher(svn_mergeinfo);
        if (matcher.find())
        {
            return Long.parseLong(matcher.group(2));
        }
        return -1;
    }

    private List<File> get_changed_files_list(final File mr, final SVNClientManager cm) throws SVNException
    {
        final List<File> fileList = new ArrayList<File>();
        cm.getStatusClient().doStatus(mr,
                                      null, /*revision*/
                                      INFINITY,
                                      false, /*remote*/
                                      false,  /*reportAll*/
                                      false, /*includeIgnored*/
                                      false, /*collectParentExternals*/
                                      new ISVNStatusHandler()
        {
            @Override
            public void handleStatus(SVNStatus status) throws SVNException
            {
                SVNStatusType statusType = status.getContentsStatus();
                if (statusType != SVNStatusType.STATUS_NONE && 
                    statusType != SVNStatusType.STATUS_NORMAL &&
                    statusType != SVNStatusType.STATUS_IGNORED)
                {
                    fileList.add(status.getFile());
                }
            }
        },
                                      null); /*changeLists */
        return fileList;
    }

    private void workspace_clean_svn_mergeinfo(final File mr, final String myself_path_rel_to_repo_root, final SVNClientManager cm, final PrintStream logger) throws SVNException
    {
        final SVNWCClient wc = cm.getWCClient();
        List<File> fileList = get_changed_files_list(mr, cm);
        fileList.add(mr);

        for (File f : fileList)
        {
            final String svn_mergeinfo = get_svn_mergeinfo(f, wc);
            if (svn_mergeinfo.isEmpty())
            {
                continue;
            }

            final String[] lines = svn_mergeinfo.split("\n");

            logger.println("Analysing svn:mergeinfo of " + f.toString());
            final StringBuilder out_svn_mergeinfo = new StringBuilder("");
            for (String l : lines)
            {
                if (l.contains(myself_path_rel_to_repo_root))
                {
                    logger.println("Dropping svn:mergeinfo line " + l);
                }
                else
                {
                    out_svn_mergeinfo.append(l);
                    out_svn_mergeinfo.append("\n");
                }
            }

            set_svn_mergeinfo(f, wc, out_svn_mergeinfo.toString());
        }
    }

    private void execute_merge(final File mr, final SVNURL mergeUrl, final long mergeRevFrom, final SVNRevision mergeRevTo, final SVNClientManager cm, final PrintStream logger) throws SVNException
    {
        final SVNDiffClient dc = cm.getDiffClient();
        final SVNWCClient wc = cm.getWCClient();

        final String myself_path_rel_to_repo_root = get_path_rel_to_repo_root(mr, wc);
        final String merge_path_rel_to_repo_root = get_path_rel_to_repo_root(mergeUrl, wc);
        final String svn_mergeinfo_pre = get_svn_mergeinfo(mr, wc);
        final long latest_merged_rev_from_mergeinfo = get_latest_merged_rev_from_mergeinfo(svn_mergeinfo_pre, merge_path_rel_to_repo_root);
        final long merge_from_opt = latest_merged_rev_from_mergeinfo > mergeRevFrom ? latest_merged_rev_from_mergeinfo : mergeRevFrom;

        logger.println("The Merge will be from " + mergeUrl + " r" + merge_from_opt + " to r" + mergeRevTo);

        workspace_clean_svn_mergeinfo(mr, myself_path_rel_to_repo_root, cm, logger);

        final SVNRevisionRange r = new SVNRevisionRange(SVNRevision.create(merge_from_opt), mergeRevTo);
        // https://svnkit.com/javadoc/org/tmatesoft/svn/core/wc/SVNDiffClient.html
        //dc.setAllowMixedRevisionsWCForMerge(true);
        dc.doMerge(mergeUrl,
                   SVNRevision.create(merge_from_opt), /*pegRevision*/
                   Arrays.asList(r),
                   mr,
                   INFINITY,
                   true,   /*useAncestry*/
                   true,   /*force*/
                   false,  /*dryRun*/
                   false); /*recordOnly*/

        workspace_clean_svn_mergeinfo(mr, myself_path_rel_to_repo_root, cm, logger);
    }

    private SVNCommitInfo execute_commit(final File mr, final String commit_msg, final SVNClientManager cm, final PrintStream logger) throws SVNException
    {
        logger.println("Committing changes");

        SVNCommitClient cc = cm.getCommitClient();
        SVNCommitInfo ci = cc.doCommit(new File[] { mr },
                                       false, /*keepLocks*/
                                       commit_msg,
                                       null,  /*revisionProperties*/
                                       null,  /*changelists*/
                                       false, /*keepChangelist*/
                                       false, /*force*/
                                       INFINITY);
        return ci;
    }

    private void execute_workspace_svn_prepare(final File mr, final SVNURL target_svn_url, final SVNClientManager cm, final PrintStream logger) throws SVNException
    {
        final SVNUpdateClient uc = cm.getUpdateClient();
        final SVNWCClient wc = cm.getWCClient();

        logger.println("Cleaning workspace of agent " + System.getenv("COMPUTERNAME") + " - " + mr);

        wc.doRevert(new File[] { mr }, INFINITY, null);
        wc.doCleanup(mr);

        logger.printf("Workspace svn URL is %s\n", wc.doInfo(mr, null).getURL());
        if (!wc.doInfo(mr, null).getURL().toString().equals(target_svn_url.toString()))
        {
            logger.println("Switching to target svn URL " + target_svn_url);
            uc.doSwitch(mr,
                        target_svn_url,
                        HEAD,
                        HEAD,
                        INFINITY,
                        true,  /*allowUnversionedObstructions*/
                        false); /*depthIsSticky*/
        }
        else
        {
            final long curr_rev = wc.doInfo(mr, null).getCommittedRevision().getNumber();
            final long head_rev = wc.doInfo(target_svn_url, HEAD, HEAD).getCommittedRevision().getNumber();
            logger.printf("Workspace is r%s\n", curr_rev);
            logger.printf("HEAD is r%s\n", head_rev);
            if (curr_rev == head_rev)
            {
                logger.printf("Workspace already to the latest revision\n");
            }
            else
            {
                logger.printf("Updating workspace to the latest revision\n");
                uc.doUpdate(mr,
                            HEAD,
                            INFINITY,
                            true,  /*allowUnversionedObstructions*/
                            false); /*depthIsSticky*/
            }
        }

        final SVNInfo wsState = wc.doInfo(mr, null);
        logger.printf("Workspace is %s r%s\n", wsState.getURL().toString() , wsState.getCommittedRevision().toString());
    }

    private boolean integrate_check_necessary(final SVNURL mergeUrl, final SVNRevision mergeRevTo, final long lastIntegrationSourceRevision, final SVNClientManager cm, final PrintStream logger) throws SVNException
    {
        final MutableBoolean changesFound = new MutableBoolean(false);
        logger.println("Check for changes after our last integration of r" + lastIntegrationSourceRevision);
        cm.getLogClient().doLog(mergeUrl,
                                null,     /*paths*/
                                mergeRevTo, /*pegRevision*/
                                mergeRevTo, /*startRevision*/
                                SVNRevision.create(lastIntegrationSourceRevision), /*endRevision*/
                                true,     /*stopOnCopy*/
                                false,    /*discoverChangedPaths*/
                                0,        /*limit*/
                                new ISVNLogEntryHandler()
        {
            public void handleLogEntry(SVNLogEntry e) throws SVNException
            {
                if (!changesFound.booleanValue())
                {
                    String message = e.getMessage();
                    if (!message.startsWith(RebaseAction.COMMIT_MESSAGE_PREFIX))
                    {
                        logger.println("Found at least a commit to be integrated: " + message);
                        changesFound.setValue(true);
                    }
                }
            }
        });
        return changesFound.booleanValue();
    }

    private void logger_print_merge_conflict(final PrintStream logger, final String mergeTo_URL, final String mergeFrom_URL)
    {
        logger.println("\n\n!!! Found conflict !!!\n");
        logger.printf( "- Checkout (or Update) %s\n", mergeTo_URL);
        logger.println("- Right click -> TortoiseSVN -> Merge");
        logger.println("  - select 'Merge a range of revisions'");
        logger.println("  - click 'Next'");
        logger.printf( "  - set 'URL to merge from' to %s\n", mergeFrom_URL);
        logger.println("  - set 'Revision range to merge' to 'all revisions'");
        logger.println("  - click 'Next'");
        logger.println("  - click 'Merge'");
        logger.println("  - click 'Edit conflict' - the merge tool will pop up");
        logger.println("  - click 'Resolved'");
        logger.println("\nAfter resolving the conflict, commit and repeat the rebase\n");
        logger.println("Work Instruction:\nhttp://mob-doc.ssluk.solomonsystech.com/QPulseDocumentService/Documents.svc/documents/active/attachment?number=MOB-O-NFI-GU-030\n");
    }

    private void logger_print_build_status(final PrintStream logger, final boolean is_success)
    {
        logger.printf("\nFinished: %s\n\n", is_success ? "SUCCESS" : "FAILURE");
    }
}
