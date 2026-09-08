Output .local/offline-demo/offline-demo.mp4
Output .local/offline-demo/offline-demo.gif

Set Shell bash
Set FontSize 20
Set Width 1400
Set Height 850
Set Padding 24
Set Theme "GruvboxDark"
Set TypingSpeed 18ms
Set Framerate 15

Hide
Type `set -o pipefail; export PS1='$ '; cd "$PYPIRON_DEMO_REPO"; clear`
Enter
Sleep 1s
Show

Type "# Prepared: one private wheel + six from PyPI."
Enter
Type "# Now give the server AND installer no external network."
Enter
Sleep 2s
Type `bash dev/scripts/offline-demo/run.sh offline |& tee "$PYPIRON_DEMO_WORK/offline-transcript.txt"`
Enter
Sleep 10s

Type "# Copy the stopped store. Check wheel hashes. Start fresh."
Enter
Sleep 2s
Type `bash dev/scripts/offline-demo/run.sh recover |& tee "$PYPIRON_DEMO_WORK/recover-transcript.txt"`
Enter
Sleep 12s

Type "# Prepared packages keep installing. Unprepared packages fail."
Enter
Type "# pypiron.com - one binary, no database"
Enter
Sleep 5s
