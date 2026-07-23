# The README quickstart demo: serve → publish → install, under a minute.
# Run via record.sh, which exports PYPIRON_DEMO_DIR (demo wheel + venv inside).
Output demo.gif

Set FontSize 19
Set Width 1250
Set Height 650
Set Padding 24
Set Theme "GruvboxDark"
Set Shell zsh
Set TypingSpeed 40ms

Hide
Type `cd "$PYPIRON_DEMO_DIR" && pkill -f "pypiron serve" ; export PYPIRON_DATA_DIR=data RUST_LOG=warn UV_PUBLISH_USERNAME=admin UV_PUBLISH_PASSWORD=demo && source .venv/bin/activate && clear`
Enter
Sleep 2s
Show

Type "uvx pypiron serve --admin-pass demo &"
Sleep 500ms
Enter
Sleep 4s

Type "uv publish --publish-url http://localhost:8080/legacy/ dist/*"
Sleep 500ms
Enter
Sleep 4s

Type "uv pip install --index-url http://localhost:8080/simple/ acme-widgets"
Sleep 500ms
Enter
Sleep 3.5s

Type "# your own PyPI in one command — private packages + proxy + mirror, no database"
Sleep 3s

Hide
Type "pkill -f 'pypiron serve'"
Enter
