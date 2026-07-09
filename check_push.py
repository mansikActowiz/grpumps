# i make this file to check push orgine command of git
print("hello git")
print("i am adding some lines..")
print("this is 3rd line of them")

"""
sarthak@sarthak-ThinkPad-T490s:~/Mansi/site_testing/gitlearning$ git clone https://github.com/mansikActowiz/grpumps.git
Cloning into 'grpumps'...
remote: Enumerating objects: 16, done.
remote: Counting objects: 100% (16/16), done.
remote: Compressing objects: 100% (13/13), done.
remote: Total 16 (delta 4), reused 3 (delta 1), pack-reused 0 (from 0)
Receiving objects: 100% (16/16), 1.60 MiB | 1.41 MiB/s, done.
Resolving deltas: 100% (4/4), done.
sarthak@sarthak-ThinkPad-T490s:~/Mansi/site_testing/gitlearning$ git remote -v
sarthak@sarthak-ThinkPad-T490s:~/Mansi/site_testing/gitlearning$ git config --global user.name "mansiKachhadiya"
sarthak@sarthak-ThinkPad-T490s:~/Mansi/site_testing/gitlearning$ git config --global user.email "mansik.actowiz@gmail.com"
sarthak@sarthak-ThinkPad-T490s:~/Mansi/site_testing/gitlearning$ git remote -v
sarthak@sarthak-ThinkPad-T490s:~/Mansi/site_testing/gitlearning$ git branck
git: 'branck' is not a git command. See 'git --help'.

The most similar command is
        branch
sarthak@sarthak-ThinkPad-T490s:~/Mansi/site_testing/gitlearning$ git branch
* master
  sub
sarthak@sarthak-ThinkPad-T490s:~/Mansi/site_testing/gitlearning$ git branch -a
* master
  sub
sarthak@sarthak-ThinkPad-T490s:~/Mansi/site_testing/gitlearning$ git status
On branch master
Changes not staged for commit:
  (use "git add <file>..." to update what will be committed)
  (use "git restore <file>..." to discard changes in working directory)
        modified:   .idea/vcs.xml
        modified:   theory_concept.py

Untracked files:
  (use "git add <file>..." to include in what will be committed)
        grpumps/

no changes added to commit (use "git add" and/or "git commit -a")
sarthak@sarthak-ThinkPad-T490s:~/Mansi/site_testing/gitlearning$ git add .
warning: adding embedded git repository: grpumps
hint: You've added another git repository inside your current repository.
hint: Clones of the outer repository will not contain the contents of
hint: the embedded repository and will not know how to obtain it.
hint: If you meant to add a submodule, use:
hint: 
hint:   git submodule add <url> grpumps
hint: 
hint: If you added this path by mistake, you can remove it from the
hint: index with:
hint: 
hint:   git rm --cached grpumps
hint: 
hint: See "git help submodule" for more information.
sarthak@sarthak-ThinkPad-T490s:~/Mansi/site_testing/gitlearning$ git rm --cached grpumps
error: the following file has staged content different from both the
file and the HEAD:
    grpumps
(use -f to force removal)
sarthak@sarthak-ThinkPad-T490s:~/Mansi/site_testing/gitlearning$ pwd
/home/sarthak/Mansi/site_testing/gitlearning
sarthak@sarthak-ThinkPad-T490s:~/Mansi/site_testing/gitlearning$ tree -a -L 2
Command 'tree' not found, but can be installed with:
sudo snap install tree  # version 2.1.3+pkg-5852, or
sudo apt  install tree  # version 2.1.1-2ubuntu3.24.04.2
See 'snap info tree' for additional versions.
sarthak@sarthak-ThinkPad-T490s:~/Mansi/site_testing/gitlearning$ rm -rf .git/
sarthak@sarthak-ThinkPad-T490s:~/Mansi/site_testing/gitlearning$ cd grpumps/
sarthak@sarthak-ThinkPad-T490s:~/Mansi/site_testing/gitlearning/grpumps$ git status
On branch main
Your branch is up to date with 'origin/main'.

nothing to commit, working tree clean
sarthak@sarthak-ThinkPad-T490s:~/Mansi/site_testing/gitlearning/grpumps$ git add .
sarthak@sarthak-ThinkPad-T490s:~/Mansi/site_testing/gitlearning/grpumps$ git stauts
git: 'stauts' is not a git command. See 'git --help'.

The most similar command is
        status
sarthak@sarthak-ThinkPad-T490s:~/Mansi/site_testing/gitlearning/grpumps$ git status
On branch main
Your branch is up to date with 'origin/main'.

Changes to be committed:
  (use "git restore --staged <file>..." to unstage)
        new file:   check_push.py

sarthak@sarthak-ThinkPad-T490s:~/Mansi/site_testing/gitlearning/grpumps$ git commit -m "new file added"
[main 8a0b720] new file added
 1 file changed, 2 insertions(+)
 create mode 100644 check_push.py
sarthak@sarthak-ThinkPad-T490s:~/Mansi/site_testing/gitlearning/grpumps$ git push origin main
Username for 'https://github.com': mansikachhadiya
Password for 'https://mansikachhadiya@github.com': 
remote: Invalid username or token. Password authentication is not supported for Git operations.
fatal: Authentication failed for 'https://github.com/mansikActowiz/grpumps.git/'
sarthak@sarthak-ThinkPad-T490s:~/Mansi/site_testing/gitlearning/grpumps$ ls -la /~.ssh
ls: cannot access '/~.ssh': No such file or directory
sarthak@sarthak-ThinkPad-T490s:~/Mansi/site_testing/gitlearning/grpumps$ ls -la ~/.ssh
total 8
drwx------  2 sarthak sarthak 4096 Jan 10 15:55 .
drwxr-x--- 31 sarthak sarthak 4096 Jul  9 15:40 ..
-rw-------  1 sarthak sarthak    0 Jan 10 15:55 authorized_keys
sarthak@sarthak-ThinkPad-T490s:~/Mansi/site_testing/gitlearning/grpumps$ git remote -v
origin  https://github.com/mansikActowiz/grpumps.git (fetch)
origin  https://github.com/mansikActowiz/grpumps.git (push)
sarthak@sarthak-ThinkPad-T490s:~/Mansi/site_testing/gitlearning/grpumps$ git push origin main
Username for 'https://github.com': 
Password for 'https://github.com': 
remote: No anonymous write access.
fatal: Authentication failed for 'https://github.com/mansikActowiz/grpumps.git/'
sarthak@sarthak-ThinkPad-T490s:~/Mansi/site_testing/gitlearning/grpumps$ git status
On branch main
Your branch is ahead of 'origin/main' by 1 commit.
  (use "git push" to publish your local commits)

nothing to commit, working tree clean
sarthak@sarthak-ThinkPad-T490s:~/Mansi/site_testing/gitlearning/grpumps$ git remote -v
origin  https://github.com/mansikActowiz/grpumps.git (fetch)
origin  https://github.com/mansikActowiz/grpumps.git (push)
sarthak@sarthak-ThinkPad-T490s:~/Mansi/site_testing/gitlearning/grpumps$ ls ~/.ssh
authorized_keys
sarthak@sarthak-ThinkPad-T490s:~/Mansi/site_testing/gitlearning/grpumps$ ssh-keygen -t ed25519 -C "mansik.actowiz@gmail.com"
Generating public/private ed25519 key pair.
Enter file in which to save the key (/home/sarthak/.ssh/id_ed25519): 
Enter passphrase (empty for no passphrase): 
Enter same passphrase again: 
Your identification has been saved in /home/sarthak/.ssh/id_ed25519
Your public key has been saved in /home/sarthak/.ssh/id_ed25519.pub
The key fingerprint is:
SHA256:qnDAcbVLmCptRJGsKqIcPlhQ40euB3vPsrCQ5IChohw mansik.actowiz@gmail.com
The key's randomart image is:
+--[ED25519 256]--+
| .oo  .          |
| .= .+ .         |
|.+o++ o          |
|==o+o. .         |
|BE*=  . S        |
|@==.o  .         |
|@++o.o.          |
|o= =..o          |
|  o oo           |
+----[SHA256]-----+
sarthak@sarthak-ThinkPad-T490s:~/Mansi/site_testing/gitlearning/grpumps$ ssh-keygen
Generating public/private ed25519 key pair.
Enter file in which to save the key (/home/sarthak/.ssh/id_ed25519): 
/home/sarthak/.ssh/id_ed25519 already exists.
Overwrite (y/n)? n
sarthak@sarthak-ThinkPad-T490s:~/Mansi/site_testing/gitlearning/grpumps$ ls -la ~/.ssh
total 16
drwx------  2 sarthak sarthak 4096 Jul  9 15:58 .
drwxr-x--- 31 sarthak sarthak 4096 Jul  9 15:40 ..
-rw-------  1 sarthak sarthak    0 Jan 10 15:55 authorized_keys
-rw-------  1 sarthak sarthak  419 Jul  9 15:58 id_ed25519
-rw-r--r--  1 sarthak sarthak  106 Jul  9 15:58 id_ed25519.pub
sarthak@sarthak-ThinkPad-T490s:~/Mansi/site_testing/gitlearning/grpumps$ cat ~/.ssh/id_ed25519.pub
ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIJsAKay5nKIcLF7kum794WoQknqS9s7qunQHgkpB0znH mansik.actowiz@gmail.com
sarthak@sarthak-ThinkPad-T490s:~/Mansi/site_testing/gitlearning/grpumps$ git remote set-url origin git@github.com:mansikActowiz/grpumps.git
sarthak@sarthak-ThinkPad-T490s:~/Mansi/site_testing/gitlearning/grpumps$ git remote -v
origin  git@github.com:mansikActowiz/grpumps.git (fetch)
origin  git@github.com:mansikActowiz/grpumps.git (push)
sarthak@sarthak-ThinkPad-T490s:~/Mansi/site_testing/gitlearning/grpumps$ ssh -T git@github.com
The authenticity of host 'github.com (20.207.73.82)' can't be established.
ED25519 key fingerprint is SHA256:+DiY3wvvV6TuJJhbpZisF/zLDA0zPMSvHdkr4UvCOqU.
This key is not known by any other names.
Are you sure you want to continue connecting (yes/no/[fingerprint])? yes
Warning: Permanently added 'github.com' (ED25519) to the list of known hosts.
Hi mansikActowiz! You've successfully authenticated, but GitHub does not provide shell access.
sarthak@sarthak-ThinkPad-T490s:~/Mansi/site_testing/gitlearning/grpumps$ git push origin main
Enumerating objects: 4, done.
Counting objects: 100% (4/4), done.
Delta compression using up to 8 threads
Compressing objects: 100% (3/3), done.
Writing objects: 100% (3/3), 343 bytes | 343.00 KiB/s, done.
Total 3 (delta 1), reused 0 (delta 0), pack-reused 0
remote: Resolving deltas: 100% (1/1), completed with 1 local object.
To github.com:mansikActowiz/grpumps.git
   5236eb9..8a0b720  main -> main
sarthak@sarthak-ThinkPad-T490s:~/Mansi/site_testing/gitlearning/grpumps$ git branch
* main
sarthak@sarthak-ThinkPad-T490s:~/Mansi/site_testing/gitlearning/grpumps$ git switch -c sub_branch
Switched to a new branch 'sub_branch'
sarthak@sarthak-ThinkPad-T490s:~/Mansi/site_testing/gitlearning/grpumps$ git branch
  main
* sub_branch
sarthak@sarthak-ThinkPad-T490s:~/Mansi/site_testing/gitlearning/grpumps$ git status
On branch sub_branch
Changes not staged for commit:
  (use "git add <file>..." to update what will be committed)
  (use "git restore <file>..." to discard changes in working directory)
        modified:   check_push.py

no changes added to commit (use "git add" and/or "git commit -a")
sarthak@sarthak-ThinkPad-T490s:~/Mansi/site_testing/gitlearning/grpumps$ git add .
sarthak@sarthak-ThinkPad-T490s:~/Mansi/site_testing/gitlearning/grpumps$ git commit -m "adding some lines"
[sub_branch 2285f77] adding some lines
 1 file changed, 3 insertions(+), 1 deletion(-)
sarthak@sarthak-ThinkPad-T490s:~/Mansi/site_testing/gitlearning/grpumps$ git push origin sub_branch
Enumerating objects: 5, done.
Counting objects: 100% (5/5), done.
Delta compression using up to 8 threads
Compressing objects: 100% (3/3), done.
Writing objects: 100% (3/3), 367 bytes | 367.00 KiB/s, done.
Total 3 (delta 1), reused 0 (delta 0), pack-reused 0
remote: Resolving deltas: 100% (1/1), completed with 1 local object.
remote: 
remote: Create a pull request for 'sub_branch' on GitHub by visiting:
remote:      https://github.com/mansikActowiz/grpumps/pull/new/sub_branch
remote: 
To github.com:mansikActowiz/grpumps.git
 * [new branch]      sub_branch -> sub_branch
sarthak@sarthak-ThinkPad-T490s:~/Mansi/site_testing/gitlearning/grpumps$ git branch
  main
* sub_branch
sarthak@sarthak-ThinkPad-T490s:~/Mansi/site_testing/gitlearning/grpumps$ git switch main
Switched to branch 'main'
Your branch is up to date with 'origin/main'.
sarthak@sarthak-ThinkPad-T490s:~/Mansi/site_testing/gitlearning/grpumps$ git branch
* main
  sub_branch
sarthak@sarthak-ThinkPad-T490s:~/Mansi/site_testing/gitlearning/grpumps$ git pull origin main 
remote: Enumerating objects: 1, done.
remote: Counting objects: 100% (1/1), done.
remote: Total 1 (delta 0), reused 0 (delta 0), pack-reused 0 (from 0)
Unpacking objects: 100% (1/1), 899 bytes | 899.00 KiB/s, done.
From github.com:mansikActowiz/grpumps
 * branch            main       -> FETCH_HEAD
   8a0b720..5766620  main       -> origin/main
Updating 8a0b720..5766620
Fast-forward
 check_push.py | 4 +++-
 1 file changed, 3 insertions(+), 1 deletion(-)
sarthak@sarthak-ThinkPad-T490s:~/Mansi/site_testing/gitlearning/grpumps$ git branch -d sub_branch 
Deleted branch sub_branch (was 2285f77).
sarthak@sarthak-ThinkPad-T490s:~/Mansi/site_testing/gitlearning/grpumps$ git push origin --delete sub_branch
To github.com:mansikActowiz/grpumps.git
 - [deleted]         sub_branch

"""