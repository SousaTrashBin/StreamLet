# Runs java StreamLet <id> for 5 nodes

for i in {0..4}; do
  gnome-terminal -- bash -c "java -cp out Streamlet $i; exec bash"
done
