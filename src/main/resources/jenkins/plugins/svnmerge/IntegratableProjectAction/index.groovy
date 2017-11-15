//  Show the last integration status
package jenkins.plugins.svnmerge.IntegratableProjectAction

import javax.swing.plaf.basic.BasicBorders.RadioButtonBorder;
import org.apache.commons.lang.StringUtils;

import static jenkins.plugins.svnmerge.RepositoryLayoutEnum.CUSTOM;

def f = namespace(lib.FormTagLib.class)
def l = namespace(lib.LayoutTagLib.class)
def t = namespace(lib.JenkinsTagLib.class)

Date date = new Date();

l.layout(norefresh:"true", title:_("title", my.project.displayName)) {
    include(my.project, "sidepanel")
    l.main_panel {
        h1 {
            img (src:"${rootURL}/plugin/svnmerge/48x48/sync.gif")
            text(_("Feature Branches"))
        }

        raw("<p>This project tracks integrations from branches via <tt>svn merge</tt></p>")

        def repoLayout = my.repositoryLayout
        p(_("Repository URL: "+repoLayout.scmModuleLocation))
        p(_("Detected repository layout: "+repoLayout.layout))
        if (StringUtils.isNotEmpty(repoLayout.subProjectName)) {
            p(_("Detected subproject name: "+repoLayout.subProjectName))
        }

        def branches = my.branches;
        if (branches.size()>0) {
            h2(_("Existing Feature Branches"))
            ul(style:"list-style:none") {
                branches.each { b ->
                    li {
                        t.jobLink(job:b)
                    }
                }
            }
        }

        h2(_("Create a new branch"))
        p(_("createBranchBlurb"))
        p {
            form (name:"new", method:"post", action:"newBranch") {

                table (width: "100%") {

                    tr {
                        td (class: "setting-leftspace")
                        td (class: "setting-name") {
                            text(_("Branch Name")+":")
                        }
                        td (class: "setting-main") {
                            input (type: "text", value: date.format("yyyyMMdd_"), name: "name", class:"setting-input")
                        }
                    }
                }

                f.submit(value:_("Create"))
            }
        }
    }
}
