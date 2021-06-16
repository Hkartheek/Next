importPackage(java.io);
importPackage(org.eclipse.jgit.transport);
importPackage(org.apache.commons.io);
importPackage(org.eclipse.jgit.api);
importPackage(java.lang);
importPackage(org.eclipse.jgit.internal.storage.file);
importPackage(org.json);


/** 
 * Function execute is the starting point 
 * Kick Start: 
 * Import Package using: importPackage 
 */ 

var debug = new StringBuffer();
function execute() {
    
    /* CATfx Options -- */
    var bitBucketURL = vGItHubUrl;
    var userName = vUserName;
    var password = vPassword;
    var repoFolderName = vFolderName;
    var branch = vBranch;
    var remote = vRemote;
    
    var localFileDirPath;
    var fileName;
    var filePath;
    var file ;
    var fileContent ;
    var localPath ;


    var jsonarray = new JSONArray(vUpload);
    for (var k1 = 0; k1 < jsonarray.length();
         k1++) {
        var jsonobject = jsonarray.getJSONObject(k1);
        localFileDirPath = jsonobject.getString("serverName");
        filePath = localFileDirPath;
        file = new File(filePath);
        fileName = file.getName();
        fileContent = FileUtils.readFileToString(file);
        localFileDirPath = localFileDirPath.substring(0,localFileDirPath.lastIndexOf("/")+1);
        localFileDirPath = localFileDirPath+repoFolderName;
        localPath = new File(localFileDirPath);
        uploadFileIntoGitHub(bitBucketURL, userName, password, localPath, repoFolderName, k1, fileName, fileContent, branch,remote);
    }
    return debug.toString();

}

function uploadFileIntoGitHub(bitBucketURL, userName,
                               password, localFileDirPath, folderName, repo_i,  fileName,  fileContent, branch, remote) {
    try{

        var git = null;
        if (repo_i === 0) {
            git = Git.cloneRepository().setURI(bitBucketURL).setCredentialsProvider(new UsernamePasswordCredentialsProvider(userName, password))
            .setDirectory(localFileDirPath)
            .call();
        }
        else {
            git = Git.open(localFileDirPath);
        }

        var repository = git.getRepository();
        var theDir = new File(repository.getDirectory().getParent(), folderName);
        if (!theDir.exists()) {
            theDir.mkdir();
        }
        var myfile = new File(theDir, fileName);
        var writer = new FileWriter(myfile);
        writer.write(fileContent);
        var spec = new RefSpec(branch + ":" + branch);
        writer.close();
        git.add().addFilepattern(".").call();
        git.commit().setMessage("Commit all changes including additions to " + fileName).call();
        git.commit().setAll(true).setMessage("Commit changes to all files").call();
        var result = git.commit().setMessage("initial commit to " + fileName).call();
        git.push().setCredentialsProvider(new UsernamePasswordCredentialsProvider(userName, password)).setRemote(remote).setRefSpecs(spec).call();
        System.out.println("\"" + fileName + "\" File Has been Pushed with commit: " + result + "\n");
        debug.append("\"" + fileName + "\" File Has been Pushed with commit: ").append(result).append("\n\n");
        git.close();
    }
    catch (e) {
        e.printStackTrace();

    }
}