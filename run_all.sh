# Runs java StreamLet <id> for 5 nodes

cd "$(dirname "$0")/out" || exit 1

for i in {0..4}; do
  gnome-terminal -- bash -c "java Streamlet $i; exec bash"
done
